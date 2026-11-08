//! Discord on the strip.
//!
//! While something needs Discord (Discord buttons or widgets in the active
//! profile, a profile that follows voice, its notifications or the muted
//! badge), a worker thread stays signed in to the Discord desktop app over
//! its local RPC socket. It follows your own voice state (channel, mute,
//! deafen, audio levels, the connection's ping), who sits and speaks in the
//! voice channels the widgets show, and the notifications Discord shows,
//! and it sends the buttons' commands. Without Discord set up in Studio it
//! stays idle and keeps no connection.

use crate::dispatch::Feedback;
use crate::log::{log_fields, Level};
use nexus_config::{ActionSpec, DiscordCommand, NexusConfig, Profile, WidgetKind};
use nexus_render::tiny_skia::Pixmap;
use nexus_render::{DiscordInfo, DiscordLink, DiscordMember, DiscordVoiceChannel};
use nexus_sensors::{
    DiscordApp, DiscordConnection, DiscordError, DiscordEvent, DiscordVoiceMember,
};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// First wait before connecting again; doubles while Discord stays away.
const RETRY_MIN: Duration = Duration::from_secs(3);
const RETRY_MAX: Duration = Duration::from_secs(30);
/// A missing authorization or an unsupported client does not fix itself
/// soon; sensors.json is still watched for a new token.
const RETRY_SLOW: Duration = Duration::from_secs(60);
const EVENT_WAIT: Duration = Duration::from_millis(100);
/// sensors.json is read this often, to follow changes from Studio.
const CONFIG_EVERY: Duration = Duration::from_secs(5);
/// Who sits where is asked again this often, in case an event was missed
/// (Discord does not send every channel's events to every client).
const RESYNC_EVERY: Duration = Duration::from_secs(20);
/// The channel list of a server is read again this often.
const CHANNELS_EVERY: Duration = Duration::from_secs(300);
/// Voice channels followed per server.
const MAX_CHANNELS: usize = 30;
/// Avatars fetched per pass through the loop, so events stay prompt.
const AVATARS_PER_PASS: usize = 2;
/// Avatars are drawn at up to 30 px; twice that keeps them sharp.
const AVATAR_PIXELS: u32 = 60;
/// Notifications waiting for the engine; older ones go when it falls behind.
const MAX_NOTIFICATIONS: usize = 8;
/// Percent per press of the volume buttons.
const VOLUME_STEP: u8 = 10;
const NOT_CONNECTED: &str = "Discord is not connected";

type LoadApp = Box<dyn Fn() -> Option<DiscordApp> + Send>;

/// What the strip needs from Discord.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Wanted {
    /// Servers named by voice widgets.
    guilds: BTreeSet<String>,
    /// A voice or call widget follows the server of the channel you are in.
    current_guild: bool,
    /// Discord's notifications are shown.
    notifications: bool,
}

/// What a button, slider or tap asks of Discord.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Request {
    Command {
        command: DiscordCommand,
        channel: Option<String>,
        step: Option<u8>,
    },
    /// Mutes or opens your microphone (push to talk).
    SetMute(bool),
    InputVolume(u8),
    OutputVolume(u16),
    /// How loud you hear someone, 0..=200 %.
    UserVolume {
        user: String,
        volume: u16,
    },
    /// Mutes someone for you.
    UserMute {
        user: String,
        mute: bool,
    },
}

struct Command {
    label: String,
    request: Request,
}

/// A message notification from Discord, ready for the strip.
pub(crate) struct DiscordNotification {
    pub(crate) title: String,
    pub(crate) body: String,
    pub(crate) icon: Option<Arc<Pixmap>>,
    /// Opens the conversation in Discord.
    pub(crate) link: Option<String>,
    /// A direct or group message, not one in a server.
    pub(crate) direct: bool,
}

#[derive(Default)]
struct Shared {
    status: Mutex<Status>,
    wake: Condvar,
}

#[derive(Default)]
struct Status {
    wanted: Option<Wanted>,
    stop: bool,
    configured: bool,
    /// Set while signed in.
    info: Option<DiscordInfo>,
    problem: Option<String>,
    notifications: VecDeque<DiscordNotification>,
    /// Discord sends its notifications here; the desktop copies of them
    /// can be left out.
    notifying: bool,
}

pub(crate) struct Discord {
    shared: Arc<Shared>,
    commands: Option<Sender<Command>>,
    /// `--dry-run-actions`: commands are logged, not sent.
    dry_run: Option<nexus_events::EventLogger>,
}

impl Discord {
    /// No worker: Discord buttons report that Discord is not connected.
    pub(crate) fn off() -> Self {
        Self {
            shared: Arc::default(),
            commands: None,
            dry_run: None,
        }
    }

    pub(crate) fn on(
        feedback: Sender<Feedback>,
        logger: nexus_events::EventLogger,
        dry_run: bool,
    ) -> Self {
        let shared = Arc::new(Shared::default());
        let (tx, rx) = mpsc::channel();
        let worker = Worker {
            shared: shared.clone(),
            commands: rx,
            feedback,
            logger: logger.clone(),
            load: Box::new(|| nexus_sensors::load_sensor_config().discord),
            avatars: HashMap::new(),
        };
        let started = thread::Builder::new()
            .name("discord".into())
            .spawn(move || worker.run());
        Self {
            shared,
            commands: started.is_ok().then_some(tx),
            dry_run: dry_run.then_some(logger),
        }
    }

    /// Connects while something is wanted, and lets go of Discord otherwise.
    pub(crate) fn set_wanted(&self, wanted: Option<Wanted>) {
        let mut status = self.lock();
        if status.wanted != wanted {
            status.wanted = wanted;
            self.shared.wake.notify_all();
        }
    }

    /// Queues a request; refused while Discord is not connected.
    pub(crate) fn send(&self, label: &str, request: Request) -> Result<(), String> {
        let (connected, configured, problem) = {
            let status = self.lock();
            let configured = status.configured && status.wanted.is_some();
            (status.info.is_some(), configured, status.problem.clone())
        };
        if !connected {
            return Err(match problem {
                Some(problem) => problem,
                None if configured => "Connecting to Discord".to_string(),
                None => format!("{NOT_CONNECTED}; set it up in Studio under Connections"),
            });
        }
        if let Some(logger) = &self.dry_run {
            log_fields(
                logger,
                Level::Info,
                "action_dry_run",
                "action executed",
                [
                    ("widget", label.to_string()),
                    ("command", format!("discord {request:?}")),
                ],
            );
            return Ok(());
        }
        let sender = self.commands.as_ref().ok_or(NOT_CONNECTED)?;
        sender
            .send(Command {
                label: label.to_string(),
                request,
            })
            .map_err(|_| NOT_CONNECTED.to_string())
    }

    /// Discord's state for the strip.
    pub(crate) fn info(&self) -> Option<DiscordInfo> {
        self.lock().info.clone()
    }

    /// Notifications that arrived since the last call.
    pub(crate) fn take_notifications(&self) -> Vec<DiscordNotification> {
        self.lock().notifications.drain(..).collect()
    }

    /// Whether Discord's notifications arrive here right now.
    pub(crate) fn notifying(&self) -> bool {
        let status = self.lock();
        status.notifying && status.info.is_some()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Status> {
        self.shared
            .status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for Discord {
    fn drop(&mut self) {
        self.lock().stop = true;
        self.shared.wake.notify_all();
    }
}

/// What the strip needs from Discord with `profile` active; None when
/// nothing.
pub(crate) fn wanted(config: &NexusConfig, profile: &Profile) -> Option<Wanted> {
    let settings = &config.settings.discord;
    let follows_voice = config.profiles.iter().any(|p| p.discord_voice);
    let mut wanted = profile_wants_discord(profile);
    if follows_voice || settings.mute_indicator || settings.notifications {
        wanted.get_or_insert_with(Wanted::default);
    }
    if let Some(wanted) = wanted.as_mut() {
        wanted.notifications = settings.notifications;
    }
    wanted
}

/// What the profile shows of Discord or does with it; None when nothing.
pub(crate) fn profile_wants_discord(profile: &Profile) -> Option<Wanted> {
    fn controls_discord(action: &ActionSpec) -> bool {
        match action {
            ActionSpec::Discord { .. } => true,
            ActionSpec::Sequence { steps, .. } => steps.iter().any(controls_discord),
            _ => false,
        }
    }
    let mut wanted = Wanted::default();
    let mut used = false;
    for widget in profile.pages.iter().flat_map(|page| &page.widgets) {
        match &widget.kind {
            WidgetKind::DiscordVoice(props) => {
                used = true;
                match &props.guild_id {
                    Some(guild) => {
                        wanted.guilds.insert(guild.clone());
                    }
                    None => wanted.current_guild = true,
                }
            }
            WidgetKind::DiscordCall(_) => {
                used = true;
                wanted.current_guild = true;
            }
            WidgetKind::Volume(props) if props.source.is_discord() => used = true,
            _ => {}
        }
        used |= [&widget.tap, &widget.double_tap, &widget.hold]
            .into_iter()
            .flatten()
            .any(controls_discord);
    }
    used.then_some(wanted)
}

/// One followed voice channel.
#[derive(Debug, Clone, Default)]
struct Channel {
    id: String,
    name: String,
    members: Vec<DiscordVoiceMember>,
}

/// Everything the worker follows while signed in.
#[derive(Debug, Default)]
struct Tracked {
    channel: Option<String>,
    guild: Option<String>,
    self_muted: bool,
    self_deafened: bool,
    /// Voice channels per followed server.
    guilds: HashMap<String, Vec<Channel>>,
    /// When each server's channel list was read.
    listed: HashMap<String, Instant>,
    /// Channels whose events are subscribed.
    subscribed: HashSet<String>,
    speaking: HashSet<String>,
    input_volume: u8,
    output_volume: u16,
    push_to_talk: bool,
    noise_suppression: bool,
    echo_cancellation: bool,
    automatic_gain: bool,
    link: Option<DiscordLink>,
    /// You, as Discord knows you.
    self_id: Option<String>,
}

impl Tracked {
    fn info(&self, avatars: &HashMap<String, Option<Arc<Pixmap>>>) -> DiscordInfo {
        let guilds = self
            .guilds
            .iter()
            .map(|(guild, channels)| {
                let channels = channels
                    .iter()
                    .map(|channel| DiscordVoiceChannel {
                        id: channel.id.clone(),
                        name: channel.name.clone(),
                        members: channel
                            .members
                            .iter()
                            .map(|member| DiscordMember {
                                user_id: member.user_id.clone(),
                                name: member.name.clone(),
                                avatar: avatars.get(&member.avatar_url).cloned().flatten(),
                                speaking: self.speaking.contains(&member.user_id),
                                muted: member.muted,
                                deafened: member.deafened,
                                volume: member.volume,
                                local_muted: member.local_muted,
                            })
                            .collect(),
                    })
                    .collect();
                (guild.clone(), channels)
            })
            .collect();
        DiscordInfo {
            channel: self.channel.clone(),
            guild: self.guild.clone(),
            self_muted: self.self_muted,
            self_deafened: self.self_deafened,
            guilds,
            input_volume: self.input_volume,
            output_volume: self.output_volume,
            push_to_talk: self.push_to_talk,
            noise_suppression: self.noise_suppression,
            echo_cancellation: self.echo_cancellation,
            automatic_gain: self.automatic_gain,
            link: self.link.filter(|_| self.channel.is_some()),
            focus: None,
            mute_badge: false,
            rotation: 0,
            self_id: self.self_id.clone(),
        }
    }

    /// Reads voice settings: a GET_VOICE_SETTINGS or SET_VOICE_SETTINGS
    /// answer, or a VOICE_SETTINGS_UPDATE event. Missing parts stay.
    fn apply_settings(&mut self, data: &Value) {
        if let Some(mute) = data["mute"].as_bool() {
            self.self_muted = mute;
        }
        if let Some(deaf) = data["deaf"].as_bool() {
            self.self_deafened = deaf;
        }
        if let Some(volume) = data["input"]["volume"].as_f64() {
            self.input_volume = volume.round().clamp(0.0, 100.0) as u8;
        }
        if let Some(volume) = data["output"]["volume"].as_f64() {
            self.output_volume = volume.round().clamp(0.0, 200.0) as u16;
        }
        if let Some(mode) = data["mode"]["type"].as_str() {
            self.push_to_talk = mode == "PUSH_TO_TALK";
        }
        for (key, value) in [
            ("noise_suppression", &mut self.noise_suppression),
            ("echo_cancellation", &mut self.echo_cancellation),
            ("automatic_gain_control", &mut self.automatic_gain),
        ] {
            if let Some(on) = data[key].as_bool() {
                *value = on;
            }
        }
    }

    /// Someone's volume or mute for you changed (a SET_USER_VOICE_SETTINGS
    /// answer).
    fn apply_user(&mut self, data: &Value) {
        let Some(user) = data["user_id"].as_str() else {
            return;
        };
        for member in self
            .guilds
            .values_mut()
            .flatten()
            .flat_map(|channel| channel.members.iter_mut())
            .filter(|member| member.user_id == user)
        {
            if let Some(volume) = data["volume"].as_f64() {
                member.volume = volume.round().clamp(0.0, 200.0) as u16;
            }
            if let Some(mute) = data["mute"].as_bool() {
                member.local_muted = mute;
            }
        }
    }

    /// Servers whose channels are followed for `wanted`.
    fn guilds_for(&self, wanted: &Wanted) -> BTreeSet<String> {
        let mut guilds = wanted.guilds.clone();
        if wanted.current_guild {
            guilds.extend(self.guild.clone());
        }
        guilds
    }

    fn channel_mut(&mut self, id: &str) -> Option<&mut Channel> {
        self.guilds
            .values_mut()
            .flat_map(|channels| channels.iter_mut())
            .find(|channel| channel.id == id)
    }

    /// Follows one event. True when someone showed up in a channel the
    /// event does not name, so the channels should be read again.
    fn apply(&mut self, event: &DiscordEvent) -> bool {
        let data = &event.data;
        match event.evt.as_str() {
            "VOICE_SETTINGS_UPDATE" => self.apply_settings(data),
            "VOICE_CHANNEL_SELECT" => {
                self.channel = data["channel_id"].as_str().map(str::to_string);
                self.guild = data["guild_id"].as_str().map(str::to_string);
                self.speaking.clear();
                if self.channel.is_none() {
                    self.link = None;
                }
            }
            "VOICE_CONNECTION_STATUS" => {
                let ping = data["last_ping"]
                    .as_f64()
                    .or_else(|| data["average_ping"].as_f64())
                    .filter(|ping| *ping > 0.0)
                    .map(|ping| ping.round() as u32);
                self.link = Some(DiscordLink {
                    connected: data["state"] == "VOICE_CONNECTED",
                    ping_ms: ping,
                });
            }
            "VOICE_STATE_CREATE" | "VOICE_STATE_UPDATE" | "VOICE_STATE_DELETE" => {
                // The channel comes with the subscription, not the event;
                // a member is found by id wherever they sit.
                let Some(member) = DiscordVoiceMember::from_voice_state(data) else {
                    return false;
                };
                let channel_id = data["channel_id"].as_str().map(str::to_string);
                if event.evt == "VOICE_STATE_DELETE" {
                    for channel in self.guilds.values_mut().flat_map(|c| c.iter_mut()) {
                        if channel_id.as_deref().is_none_or(|id| id == channel.id) {
                            channel.members.retain(|m| m.user_id != member.user_id);
                        }
                    }
                    self.speaking.remove(&member.user_id);
                    return false;
                }
                let mut updated = false;
                for channel in self.guilds.values_mut().flat_map(|c| c.iter_mut()) {
                    if let Some(existing) = channel
                        .members
                        .iter_mut()
                        .find(|m| m.user_id == member.user_id)
                    {
                        *existing = member.clone();
                        updated = true;
                    }
                }
                if !updated {
                    match channel_id.as_deref().and_then(|id| self.channel_mut(id)) {
                        Some(channel) => channel.members.push(member),
                        None => return true,
                    }
                }
            }
            "SPEAKING_START" => {
                if let Some(user) = data["user_id"].as_str() {
                    self.speaking.insert(user.to_string());
                }
            }
            "SPEAKING_STOP" => {
                if let Some(user) = data["user_id"].as_str() {
                    self.speaking.remove(user);
                }
            }
            _ => {}
        }
        false
    }
}

const CHANNEL_EVENTS: [&str; 5] = [
    "VOICE_STATE_CREATE",
    "VOICE_STATE_UPDATE",
    "VOICE_STATE_DELETE",
    "SPEAKING_START",
    "SPEAKING_STOP",
];

struct Worker {
    shared: Arc<Shared>,
    commands: Receiver<Command>,
    feedback: Sender<Feedback>,
    logger: nexus_events::EventLogger,
    load: LoadApp,
    /// Decoded pictures by URL; None when one could not be fetched.
    avatars: HashMap<String, Option<Arc<Pixmap>>>,
}

impl Worker {
    fn lock(&self) -> std::sync::MutexGuard<'_, Status> {
        self.shared
            .status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn wanted(&self) -> Option<Option<Wanted>> {
        let status = self.lock();
        (!status.stop).then(|| status.wanted.clone())
    }

    fn run(mut self) {
        let mut retry = RETRY_MIN;
        let mut retry_at = Instant::now();
        loop {
            let Some(wanted) = self.wanted() else {
                return;
            };
            while let Ok(command) = self.commands.try_recv() {
                self.not_sent(command, NOT_CONNECTED);
            }
            let app = if wanted.is_some() && Instant::now() >= retry_at {
                let app = (self.load)();
                self.lock().configured = app.is_some();
                if app.is_none() {
                    // Not set up: look again now and then.
                    retry_at = Instant::now() + CONFIG_EVERY;
                }
                app
            } else {
                None
            };
            let Some(app) = app else {
                if wanted.is_none() {
                    self.lock().problem = None;
                }
                self.wait(Duration::from_secs(1));
                continue;
            };
            match nexus_sensors::discord_sign_in(&app, CONNECT_TIMEOUT) {
                Ok((connection, user, renewed)) => {
                    retry = RETRY_MIN;
                    let mut app = app;
                    if let Some(token) = renewed {
                        if let Err(error) =
                            nexus_sensors::store_discord_token(&app.client_id, token.clone())
                        {
                            log_fields(
                                &self.logger,
                                Level::Warn,
                                "discord_token_not_saved",
                                "could not save the renewed Discord token",
                                [("error", error.to_string())],
                            );
                        }
                        app.token = Some(token);
                    }
                    self.session(connection, &app, &user);
                    retry_at = Instant::now() + RETRY_MIN;
                }
                Err(error) => {
                    let wait = match error {
                        DiscordError::NotAuthorized
                        | DiscordError::Unsupported
                        | DiscordError::UnknownApplication => RETRY_SLOW,
                        _ => retry,
                    };
                    retry = (retry * 2).min(RETRY_MAX);
                    retry_at = Instant::now() + wait;
                    let problem = error.to_string();
                    let changed = self.lock().problem.as_deref() != Some(problem.as_str());
                    if changed {
                        log_fields(
                            &self.logger,
                            Level::Warn,
                            "discord_unavailable",
                            "could not connect to Discord",
                            [("error", problem.clone())],
                        );
                    }
                    self.lock().problem = Some(problem);
                }
            }
        }
    }

    /// Sleeps up to `wait`, or until wanted or stop change.
    fn wait(&self, wait: Duration) {
        let status = self.lock();
        let _ = self.shared.wake.wait_timeout(status, wait);
    }

    fn session(&mut self, mut discord: DiscordConnection, app: &DiscordApp, user: &str) {
        let mut tracked = Tracked {
            self_id: discord.user_id().map(str::to_string),
            ..Tracked::default()
        };
        let started = Self::sync_self(&mut discord, &mut tracked).and_then(|()| {
            discord.subscribe("VOICE_SETTINGS_UPDATE", json!({}))?;
            discord.subscribe("VOICE_CHANNEL_SELECT", json!({}))?;
            // Older Discord versions may not know it; the ping just stays
            // unknown then.
            match discord.subscribe("VOICE_CONNECTION_STATUS", json!({})) {
                Ok(()) | Err(DiscordError::Command { .. }) => Ok(()),
                Err(error) => Err(error),
            }
        });
        if let Err(error) = started {
            self.lock().problem = Some(error.to_string());
            return;
        }
        log_fields(
            &self.logger,
            Level::Info,
            "discord_connected",
            "connected to Discord",
            [("user", user.to_string())],
        );
        self.lock().problem = None;
        let may_notify = app
            .token
            .as_ref()
            .is_some_and(|token| token.has_scope(nexus_sensors::DISCORD_NOTIFICATIONS_SCOPE));
        let mut notifying = false;
        let mut notify_refused = false;
        let mut config_checked = Instant::now();
        let mut resync_at = Instant::now();
        let ended = loop {
            let Some(Some(wanted)) = self.wanted() else {
                break None;
            };
            let notify = wanted.notifications && may_notify && !notify_refused;
            if notify != notifying {
                let result = if notify {
                    discord.subscribe("NOTIFICATION_CREATE", json!({}))
                } else {
                    discord.unsubscribe("NOTIFICATION_CREATE", json!({}))
                };
                match result {
                    Ok(()) => notifying = notify,
                    Err(DiscordError::Command { message, .. }) => {
                        if notify {
                            notify_refused = true;
                            log_fields(
                                &self.logger,
                                Level::Warn,
                                "discord_notifications_refused",
                                "Discord does not send its notifications",
                                [("error", message)],
                            );
                        }
                        notifying = false;
                    }
                    Err(error) => break Some(error),
                }
                self.lock().notifying = notifying;
            }
            if config_checked.elapsed() >= CONFIG_EVERY {
                config_checked = Instant::now();
                let stored = (self.load)();
                let same = stored.as_ref().is_some_and(|stored| {
                    stored.client_id == app.client_id
                        && stored.token.as_ref().map(|t| &t.access_token)
                            == app.token.as_ref().map(|t| &t.access_token)
                });
                if !same {
                    break None;
                }
            }
            if let Err(error) = self.run_commands(&mut discord, &mut tracked) {
                break Some(error);
            }
            if Instant::now() >= resync_at {
                resync_at = Instant::now() + RESYNC_EVERY;
                if let Err(error) = Self::sync_self(&mut discord, &mut tracked)
                    .and_then(|()| Self::follow(&mut discord, &mut tracked, &wanted, true))
                {
                    break Some(error);
                }
            } else if let Err(error) = Self::follow(&mut discord, &mut tracked, &wanted, false) {
                break Some(error);
            }
            let mut resync_soon = false;
            let mut messages = Vec::new();
            match discord.next_event(EVENT_WAIT) {
                Ok(Some(event)) => {
                    let mut event = Some(event);
                    // Take what else arrived before redrawing.
                    while let Some(next) = event.take() {
                        if next.evt == "NOTIFICATION_CREATE" {
                            messages.push(next.data);
                        } else {
                            resync_soon |= next.evt == "VOICE_CHANNEL_SELECT";
                            resync_soon |= tracked.apply(&next);
                        }
                        // The next read reports a broken connection.
                        event = discord.next_event(Duration::ZERO).ok().flatten();
                    }
                }
                Ok(None) => {}
                Err(error) => break Some(error),
            }
            let mut lost = None;
            for message in messages.into_iter().filter(|_| notifying) {
                match self.notification(&mut discord, &message) {
                    Ok(notification) => {
                        let mut status = self.lock();
                        if status.notifications.len() >= MAX_NOTIFICATIONS {
                            status.notifications.pop_front();
                        }
                        status.notifications.push_back(notification);
                    }
                    Err(error) => {
                        lost = Some(error);
                        break;
                    }
                }
            }
            if lost.is_some() {
                break lost;
            }
            if resync_soon {
                // Members of a channel you joined, or of one someone
                // joined, show within a moment.
                resync_at = resync_at.min(Instant::now() + Duration::from_millis(500));
            }
            self.fetch_avatars(&tracked);
            self.lock().info = Some(tracked.info(&self.avatars));
        };
        {
            let mut status = self.lock();
            status.info = None;
            status.notifying = false;
        }
        self.ended(ended);
    }

    /// Turns a NOTIFICATION_CREATE into what the strip shows: the author's
    /// picture, and a link to the message (asking Discord which server the
    /// channel belongs to; none for direct messages).
    fn notification(
        &mut self,
        discord: &mut DiscordConnection,
        data: &Value,
    ) -> Result<DiscordNotification, DiscordError> {
        let text = |value: &Value| value.as_str().unwrap_or_default().trim().to_string();
        let channel = data["channel_id"].as_str().map(str::to_string);
        let answer = match &channel {
            Some(channel) => match discord.command("GET_CHANNEL", json!({ "channel_id": channel }))
            {
                Ok(answer) => answer,
                Err(DiscordError::Command { .. }) => Value::Null,
                Err(error) => return Err(error),
            },
            None => Value::Null,
        };
        let guild = answer["guild_id"].as_str().map(str::to_string);
        // 1 is a direct message, 3 a group; without an answer, a message
        // outside any server.
        let direct = match answer["type"].as_u64() {
            Some(kind) => matches!(kind, 1 | 3),
            None => guild.is_none(),
        };
        let link = channel.and_then(|channel| {
            let place = guild.unwrap_or_else(|| "@me".to_string());
            let mut link = format!("discord://-/channels/{place}/{channel}");
            if let Some(message) = data["message"]["id"].as_str() {
                link.push('/');
                link.push_str(message);
            }
            nexus_actions::is_discord_link(&link).then_some(link)
        });
        let icon_url = data["icon_url"]
            .as_str()
            .filter(|url| url.starts_with("https://"))
            .map(str::to_string);
        let icon = icon_url.and_then(|url| self.avatar(&url));
        Ok(DiscordNotification {
            title: text(&data["title"]),
            body: text(&data["body"]),
            icon,
            link,
            direct,
        })
    }

    /// A picture by URL, fetched once.
    fn avatar(&mut self, url: &str) -> Option<Arc<Pixmap>> {
        if let Some(known) = self.avatars.get(url) {
            return known.clone();
        }
        let image = nexus_sensors::fetch_art_bytes(url)
            .and_then(|bytes| nexus_render::icons::decode_image(&bytes, AVATAR_PIXELS))
            .map(Arc::new);
        self.avatars.insert(url.to_string(), image.clone());
        image
    }

    fn ended(&self, error: Option<DiscordError>) {
        self.lock().problem = error.as_ref().map(ToString::to_string);
        log_fields(
            &self.logger,
            Level::Info,
            "discord_disconnected",
            "disconnected from Discord",
            [(
                "reason",
                error.map_or_else(|| "no longer needed".into(), |error| error.to_string()),
            )],
        );
    }

    /// Your own channel, mute and deafen.
    fn sync_self(
        discord: &mut DiscordConnection,
        tracked: &mut Tracked,
    ) -> Result<(), DiscordError> {
        let settings = discord.command("GET_VOICE_SETTINGS", json!({}))?;
        tracked.apply_settings(&settings);
        let selected = discord.command("GET_SELECTED_VOICE_CHANNEL", json!({}))?;
        let channel = selected["id"].as_str().map(str::to_string);
        if channel != tracked.channel {
            tracked.speaking.clear();
        }
        tracked.channel = channel;
        tracked.guild = selected["guild_id"].as_str().map(str::to_string);
        Ok(())
    }

    /// Reads the channels of the servers in `wanted` (when new, stale or
    /// `resync`), who sits in them, and subscribes to their events. Servers
    /// no longer wanted are dropped.
    fn follow(
        discord: &mut DiscordConnection,
        tracked: &mut Tracked,
        wanted: &Wanted,
        resync: bool,
    ) -> Result<(), DiscordError> {
        let guilds = tracked.guilds_for(wanted);
        let dropped: Vec<String> = tracked
            .guilds
            .keys()
            .filter(|guild| !guilds.contains(*guild))
            .cloned()
            .collect();
        for guild in dropped {
            for channel in tracked.guilds.remove(&guild).unwrap_or_default() {
                if tracked.subscribed.remove(&channel.id) {
                    for evt in CHANNEL_EVENTS {
                        // An error here only means it was not subscribed.
                        let _ = discord.unsubscribe(evt, json!({ "channel_id": channel.id }));
                    }
                }
            }
            tracked.listed.remove(&guild);
        }
        for guild in guilds {
            let stale = tracked
                .listed
                .get(&guild)
                .is_none_or(|at| at.elapsed() >= CHANNELS_EVERY);
            if !stale && !resync {
                continue;
            }
            if stale {
                let answer = match discord.command("GET_CHANNELS", json!({ "guild_id": guild })) {
                    Ok(answer) => answer,
                    // Not a member any more, or not allowed: show nothing.
                    Err(DiscordError::Command { .. }) => Value::Null,
                    Err(error) => return Err(error),
                };
                let listed = nexus_sensors::discord_voice_channels(&answer);
                let old = tracked.guilds.remove(&guild).unwrap_or_default();
                let channels = listed
                    .into_iter()
                    .take(MAX_CHANNELS)
                    .map(|channel| Channel {
                        members: old
                            .iter()
                            .find(|o| o.id == channel.id)
                            .map(|o| o.members.clone())
                            .unwrap_or_default(),
                        id: channel.id,
                        name: channel.name,
                    })
                    .collect();
                tracked.guilds.insert(guild.clone(), channels);
                tracked.listed.insert(guild.clone(), Instant::now());
            }
            let ids: Vec<String> = tracked.guilds[&guild]
                .iter()
                .map(|c| c.id.clone())
                .collect();
            for id in ids {
                let members = match discord.command("GET_CHANNEL", json!({ "channel_id": id })) {
                    Ok(answer) => answer["voice_states"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(DiscordVoiceMember::from_voice_state)
                        .collect(),
                    Err(DiscordError::Command { .. }) => Vec::new(),
                    Err(error) => return Err(error),
                };
                if let Some(channel) = tracked.channel_mut(&id) {
                    channel.members = members;
                }
                if tracked.subscribed.insert(id.clone()) {
                    for evt in CHANNEL_EVENTS {
                        match discord.subscribe(evt, json!({ "channel_id": id })) {
                            Ok(()) | Err(DiscordError::Command { .. }) => {}
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Fetches a few missing pictures.
    fn fetch_avatars(&mut self, tracked: &Tracked) {
        let missing: Vec<String> = tracked
            .guilds
            .values()
            .flatten()
            .flat_map(|channel| &channel.members)
            .map(|member| member.avatar_url.clone())
            .filter(|url| !self.avatars.contains_key(url))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .take(AVATARS_PER_PASS)
            .collect();
        for url in missing {
            self.avatar(&url);
        }
    }

    /// Reports on the strip that a command did not reach Discord.
    fn not_sent(&self, command: Command, why: &str) {
        log_fields(
            &self.logger,
            Level::Error,
            "action_failed",
            "action failed",
            [
                ("widget", command.label.clone()),
                ("error", why.to_string()),
            ],
        );
        let _ = self.feedback.send(Feedback {
            label: command.label,
            error: why.to_string(),
        });
    }

    /// Sends the queued commands. Discord refusing one is reported on the
    /// strip; a broken connection ends the session.
    fn run_commands(
        &self,
        discord: &mut DiscordConnection,
        tracked: &mut Tracked,
    ) -> Result<(), DiscordError> {
        while let Ok(command) = self.commands.try_recv() {
            let Some((cmd, args)) = request(&command.request, tracked) else {
                continue;
            };
            match discord.command(cmd, args) {
                Ok(answer) => {
                    match cmd {
                        "SET_VOICE_SETTINGS" => tracked.apply_settings(&answer),
                        "SET_USER_VOICE_SETTINGS" => tracked.apply_user(&answer),
                        "SELECT_VOICE_CHANNEL" => {
                            tracked.channel = answer["id"].as_str().map(str::to_string);
                            tracked.guild = answer["guild_id"].as_str().map(str::to_string);
                            tracked.speaking.clear();
                            if tracked.channel.is_none() {
                                tracked.link = None;
                            }
                        }
                        _ => {}
                    }
                    log_fields(
                        &self.logger,
                        Level::Info,
                        "action_executed",
                        "action executed",
                        [
                            ("widget", command.label),
                            ("command", format!("discord {cmd}")),
                        ],
                    );
                }
                Err(error @ DiscordError::Command { .. }) => {
                    self.not_sent(command, &error.to_string())
                }
                Err(error) => {
                    self.not_sent(command, &error.to_string());
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}

/// The RPC command for a request; None when there is nothing to do
/// (joining the channel you are already in, push to talk, which the engine
/// turns into mute and unmute).
fn request(request: &Request, tracked: &Tracked) -> Option<(&'static str, Value)> {
    let (command, channel, step) = match request {
        Request::Command {
            command,
            channel,
            step,
        } => (*command, channel.as_deref(), *step),
        Request::SetMute(mute) => return Some(("SET_VOICE_SETTINGS", json!({ "mute": mute }))),
        Request::InputVolume(volume) => {
            return Some((
                "SET_VOICE_SETTINGS",
                json!({ "input": { "volume": (*volume).min(100) } }),
            ))
        }
        Request::OutputVolume(volume) => {
            return Some((
                "SET_VOICE_SETTINGS",
                json!({ "output": { "volume": (*volume).min(200) } }),
            ))
        }
        Request::UserVolume { user, volume } => {
            return Some((
                "SET_USER_VOICE_SETTINGS",
                json!({ "user_id": user, "volume": (*volume).min(200) }),
            ))
        }
        Request::UserMute { user, mute } => {
            return Some((
                "SET_USER_VOICE_SETTINGS",
                json!({ "user_id": user, "mute": mute }),
            ))
        }
    };
    let step = i32::from(step.unwrap_or(VOLUME_STEP).clamp(1, 100));
    let input = |delta: i32| {
        let volume = (i32::from(tracked.input_volume) + delta).clamp(0, 100);
        (
            "SET_VOICE_SETTINGS",
            json!({ "input": { "volume": volume } }),
        )
    };
    let output = |delta: i32| {
        let volume = (i32::from(tracked.output_volume) + delta).clamp(0, 200);
        (
            "SET_VOICE_SETTINGS",
            json!({ "output": { "volume": volume } }),
        )
    };
    Some(match command {
        DiscordCommand::ToggleMute => {
            ("SET_VOICE_SETTINGS", json!({ "mute": !tracked.self_muted }))
        }
        DiscordCommand::ToggleDeafen => (
            "SET_VOICE_SETTINGS",
            json!({ "deaf": !tracked.self_deafened }),
        ),
        DiscordCommand::JoinVoice => {
            let channel = channel?;
            if tracked.channel.as_deref() == Some(channel) {
                return None;
            }
            // `force` moves you out of another channel; the tap on the
            // strip is the user's approval to do so.
            (
                "SELECT_VOICE_CHANNEL",
                json!({ "channel_id": channel, "force": true }),
            )
        }
        DiscordCommand::LeaveVoice => ("SELECT_VOICE_CHANNEL", json!({ "channel_id": null })),
        DiscordCommand::PushToTalk => return None,
        DiscordCommand::InputVolumeUp => input(step),
        DiscordCommand::InputVolumeDown => input(-step),
        DiscordCommand::OutputVolumeUp => output(step),
        DiscordCommand::OutputVolumeDown => output(-step),
        DiscordCommand::ToggleVoiceMode => {
            let mode = if tracked.push_to_talk {
                "VOICE_ACTIVITY"
            } else {
                "PUSH_TO_TALK"
            };
            ("SET_VOICE_SETTINGS", json!({ "mode": { "type": mode } }))
        }
        DiscordCommand::ToggleNoiseSuppression => (
            "SET_VOICE_SETTINGS",
            json!({ "noise_suppression": !tracked.noise_suppression }),
        ),
        DiscordCommand::ToggleEchoCancellation => (
            "SET_VOICE_SETTINGS",
            json!({ "echo_cancellation": !tracked.echo_cancellation }),
        ),
        DiscordCommand::ToggleAutomaticGain => (
            "SET_VOICE_SETTINGS",
            json!({ "automatic_gain_control": !tracked.automatic_gain }),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexus_config::{ButtonProps, DiscordVoiceProps, Widget};

    fn member(id: &str) -> Value {
        json!({ "user": { "id": id, "username": format!("u{id}") }, "voice_state": {} })
    }

    fn tracked() -> Tracked {
        let mut tracked = Tracked {
            channel: Some("10".into()),
            guild: Some("1".into()),
            ..Tracked::default()
        };
        tracked.guilds.insert(
            "1".into(),
            vec![
                Channel {
                    id: "10".into(),
                    name: "Gaming".into(),
                    members: vec![DiscordVoiceMember::from_voice_state(&member("100")).unwrap()],
                },
                Channel {
                    id: "11".into(),
                    name: "Chill".into(),
                    members: Vec::new(),
                },
            ],
        );
        tracked
    }

    fn event(evt: &str, data: Value) -> DiscordEvent {
        DiscordEvent {
            evt: evt.into(),
            data,
        }
    }

    #[test]
    fn follows_members_moving_speaking_and_leaving() {
        let mut tracked = tracked();
        let mut join = member("200");
        join["channel_id"] = "11".into();
        tracked.apply(&event("VOICE_STATE_CREATE", join));
        tracked.apply(&event("SPEAKING_START", json!({ "user_id": "100" })));
        let info = tracked.info(&HashMap::new());
        let channels = info.channels_for(None);
        assert_eq!(channels[1].members[0].user_id, "200");
        assert!(channels[0].members[0].speaking);

        let mut muted = member("200");
        muted["voice_state"]["self_mute"] = true.into();
        tracked.apply(&event("VOICE_STATE_UPDATE", muted));
        assert!(tracked.guilds["1"][1].members[0].muted);

        tracked.apply(&event("VOICE_STATE_DELETE", member("100")));
        assert!(tracked.guilds["1"][0].members.is_empty());
        assert!(tracked.speaking.is_empty());

        tracked.apply(&event(
            "VOICE_SETTINGS_UPDATE",
            json!({ "mute": true, "deaf": false }),
        ));
        assert!(tracked.self_muted);
        tracked.apply(&event(
            "VOICE_CHANNEL_SELECT",
            json!({ "channel_id": null, "guild_id": null }),
        ));
        assert_eq!(tracked.channel, None);
    }

    fn command(command: DiscordCommand, channel: Option<&str>) -> Request {
        Request::Command {
            command,
            channel: channel.map(Into::into),
            step: None,
        }
    }

    #[test]
    fn commands_become_rpc_requests() {
        let mut tracked = tracked();
        assert_eq!(
            request(&command(DiscordCommand::ToggleMute, None), &tracked),
            Some(("SET_VOICE_SETTINGS", json!({ "mute": true })))
        );
        assert_eq!(
            request(&command(DiscordCommand::JoinVoice, Some("10")), &tracked),
            None
        );
        assert_eq!(
            request(&command(DiscordCommand::JoinVoice, Some("11")), &tracked),
            Some((
                "SELECT_VOICE_CHANNEL",
                json!({ "channel_id": "11", "force": true })
            ))
        );
        assert_eq!(
            request(&command(DiscordCommand::LeaveVoice, None), &tracked)
                .unwrap()
                .1,
            json!({ "channel_id": null })
        );
        assert_eq!(
            request(&command(DiscordCommand::PushToTalk, None), &tracked),
            None
        );

        // Levels step from where they are and stay in range.
        tracked.input_volume = 95;
        tracked.output_volume = 100;
        assert_eq!(
            request(&command(DiscordCommand::InputVolumeUp, None), &tracked)
                .unwrap()
                .1,
            json!({ "input": { "volume": 100 } })
        );
        let louder = Request::Command {
            command: DiscordCommand::OutputVolumeUp,
            channel: None,
            step: Some(25),
        };
        assert_eq!(
            request(&louder, &tracked).unwrap().1,
            json!({ "output": { "volume": 125 } })
        );
        assert_eq!(
            request(&command(DiscordCommand::ToggleVoiceMode, None), &tracked)
                .unwrap()
                .1,
            json!({ "mode": { "type": "PUSH_TO_TALK" } })
        );
        assert_eq!(
            request(
                &Request::UserVolume {
                    user: "7".into(),
                    volume: 250
                },
                &tracked
            ),
            Some((
                "SET_USER_VOICE_SETTINGS",
                json!({ "user_id": "7", "volume": 200 })
            ))
        );
    }

    #[test]
    fn follows_settings_the_connection_and_volumes_for_you() {
        let mut tracked = tracked();
        tracked.apply(&event(
            "VOICE_SETTINGS_UPDATE",
            json!({
                "input": { "volume": 64.4 },
                "output": { "volume": 150 },
                "mode": { "type": "PUSH_TO_TALK" },
                "noise_suppression": true,
                "automatic_gain_control": false,
            }),
        ));
        assert_eq!((tracked.input_volume, tracked.output_volume), (64, 150));
        assert!(tracked.push_to_talk && tracked.noise_suppression && !tracked.automatic_gain);

        tracked.apply(&event(
            "VOICE_CONNECTION_STATUS",
            json!({ "state": "VOICE_CONNECTED", "last_ping": 41.6, "average_ping": 50 }),
        ));
        let info = tracked.info(&HashMap::new());
        assert_eq!(
            info.link,
            Some(DiscordLink {
                connected: true,
                ping_ms: Some(42)
            })
        );

        tracked.apply_user(&json!({ "user_id": "100", "volume": 40, "mute": true }));
        let member = &tracked.guilds["1"][0].members[0];
        assert_eq!((member.volume, member.local_muted), (40, true));

        tracked.apply(&event(
            "VOICE_CHANNEL_SELECT",
            json!({ "channel_id": null, "guild_id": null }),
        ));
        assert_eq!(tracked.info(&HashMap::new()).link, None);
    }

    #[test]
    fn the_profile_says_which_servers_to_follow() {
        let mut profile = nexus_config::default_config().profiles.remove(0);
        assert_eq!(profile_wants_discord(&profile), None);
        let mut voice = Widget::new(
            "voice",
            0,
            300,
            WidgetKind::DiscordVoice(DiscordVoiceProps {
                guild_id: Some("5".into()),
                ..DiscordVoiceProps::default()
            }),
        );
        profile.pages[0].widgets = vec![voice.clone()];
        let wanted = profile_wants_discord(&profile).unwrap();
        assert_eq!(wanted.guilds, BTreeSet::from(["5".to_string()]));
        assert!(!wanted.current_guild);

        voice.kind = WidgetKind::Button(ButtonProps::default());
        voice.tap = Some(ActionSpec::Sequence {
            steps: vec![ActionSpec::Discord {
                command: DiscordCommand::ToggleMute,
                channel: None,
                step: None,
            }],
            delay_ms: 0,
        });
        profile.pages[0].widgets = vec![voice.clone()];
        assert_eq!(profile_wants_discord(&profile), Some(Wanted::default()));

        voice.tap = None;
        voice.kind = WidgetKind::DiscordCall(nexus_config::DiscordCallProps::default());
        profile.pages[0].widgets = vec![voice];
        assert!(profile_wants_discord(&profile).unwrap().current_guild);
    }

    #[test]
    fn notifications_badge_and_voice_profiles_keep_discord_connected() {
        let mut config = nexus_config::default_config();
        let profile = config.profiles[0].clone();
        assert_eq!(
            wanted(&config, &profile),
            Some(Wanted {
                notifications: true,
                ..Wanted::default()
            })
        );
        config.settings.discord.notifications = false;
        config.settings.discord.mute_indicator = false;
        assert_eq!(wanted(&config, &profile), None);
        config.profiles[0].discord_voice = true;
        assert_eq!(wanted(&config, &profile), Some(Wanted::default()));
    }
}
