//! Touch calibration: the strip shows marks one after the other, the user
//! touches each, and the panel's raw readings give the correction to store
//! in `settings.touch`.
//!
//! Each mark is touched twice (two rounds across the strip), since a finger
//! lands within about a dozen pixels of where it aims. A straight line fit
//! through all touches gives the correction at both edges.

use nexus_ipc::{CalibrationState, CalibrationStatus, TouchPhase};
use std::time::{Duration, Instant};

/// Where the marks stand, left to right.
pub(crate) const MARKS: [u16; 5] = [40, 180, 320, 460, 600];
const ROUNDS: usize = 2;
/// The calibration gives up after this long without a touch.
const IDLE_LIMIT: Duration = Duration::from_secs(30);
/// A fit that stretches the strip more than this is not a correction but a
/// series of stray touches.
const MAX_STRETCH: f32 = 0.25;
/// Largest correction stored at either edge, as the config allows.
const MAX_CORRECTION: f32 = nexus_config::MAX_TOUCH_CORRECTION as f32;

pub(crate) struct Calibration {
    /// (mark, raw X the panel read).
    samples: Vec<(u16, u16)>,
    /// X where the finger came down, until it lifts.
    down: Option<u16>,
    last_input: Instant,
}

impl Calibration {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            samples: Vec::new(),
            down: None,
            last_input: now,
        }
    }

    pub(crate) fn steps() -> usize {
        MARKS.len() * ROUNDS
    }

    /// The mark to touch now, or None once all were touched.
    pub(crate) fn mark(&self) -> Option<u16> {
        (self.samples.len() < Self::steps()).then(|| MARKS[self.samples.len() % MARKS.len()])
    }

    pub(crate) fn step(&self) -> usize {
        self.samples.len()
    }

    /// A touch on the panel, at its raw X. The X where the finger came
    /// down counts: the panel reports a lift where the touch was last seen.
    pub(crate) fn touch(&mut self, phase: TouchPhase, raw_x: u16, now: Instant) {
        self.last_input = now;
        match phase {
            TouchPhase::Began => self.down = Some(raw_x),
            TouchPhase::Moved => {}
            TouchPhase::Ended => {
                if let (Some(x), Some(mark)) = (self.down.take(), self.mark()) {
                    self.samples.push((mark, x));
                }
            }
        }
    }

    pub(crate) fn timed_out(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_input) >= IDLE_LIMIT
    }

    /// The correction at the left and right edges, once every mark was
    /// touched; None when the touches do not fit a correction.
    pub(crate) fn result(&self) -> Option<(i16, i16)> {
        if self.mark().is_some() {
            return None;
        }
        fit(&self.samples)
    }

    pub(crate) fn status(&self) -> CalibrationStatus {
        CalibrationStatus {
            state: CalibrationState::Running,
            step: self.step(),
            steps: Self::steps(),
            left: None,
            right: None,
        }
    }
}

/// Least squares line `mark = a * raw + b`, as the correction it means at
/// raw X 0 and 639.
fn fit(samples: &[(u16, u16)]) -> Option<(i16, i16)> {
    let n = samples.len() as f32;
    if n < 2.0 {
        return None;
    }
    let mean =
        |pick: fn(&(u16, u16)) -> u16| samples.iter().map(|s| f32::from(pick(s))).sum::<f32>() / n;
    let (mark_mean, raw_mean) = (mean(|s| s.0), mean(|s| s.1));
    let spread: f32 = samples
        .iter()
        .map(|&(_, raw)| (f32::from(raw) - raw_mean).powi(2))
        .sum();
    if spread < 1.0 {
        return None;
    }
    let a = samples
        .iter()
        .map(|&(mark, raw)| (f32::from(raw) - raw_mean) * (f32::from(mark) - mark_mean))
        .sum::<f32>()
        / spread;
    if (a - 1.0).abs() > MAX_STRETCH {
        return None;
    }
    let b = mark_mean - a * raw_mean;
    let last = f32::from(nexus_config::STRIP_WIDTH - 1);
    let left = b;
    let right = a * last + b - last;
    if left.abs() > MAX_CORRECTION || right.abs() > MAX_CORRECTION {
        return None;
    }
    Some((left.round() as i16, right.round() as i16))
}

/// Status of a calibration that ended.
pub(crate) fn ended(state: CalibrationState, result: Option<(i16, i16)>) -> CalibrationStatus {
    CalibrationStatus {
        state,
        step: if state == CalibrationState::Done {
            Calibration::steps()
        } else {
            0
        },
        steps: Calibration::steps(),
        left: result.map(|r| r.0),
        right: result.map(|r| r.1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tap(calibration: &mut Calibration, x: u16, now: Instant) {
        calibration.touch(TouchPhase::Began, x, now);
        calibration.touch(TouchPhase::Moved, x + 3, now);
        calibration.touch(TouchPhase::Ended, x + 5, now);
    }

    #[test]
    fn walks_the_marks_twice_and_measures_the_correction() {
        let now = Instant::now();
        let mut calibration = Calibration::new(now);
        assert_eq!(calibration.mark(), Some(40));
        // A panel like the one measured by hand: 28 px short at the left,
        // 11 px at the right, and a finger a few pixels off either way.
        let wobble = [3i32, -4, 2, -1, 4, -3, 1, -2, 3, -3];
        for (step, jitter) in wobble.iter().enumerate() {
            let mark = calibration.mark().unwrap();
            let actual = f32::from(mark);
            // Solve x + 28 - 17 x / 639 = actual for the reported x.
            let reported = (actual - 28.0) / (1.0 - 17.0 / 639.0);
            tap(&mut calibration, (reported as i32 + jitter) as u16, now);
            assert_eq!(calibration.step(), step + 1);
        }
        assert_eq!(calibration.mark(), None);
        let (left, right) = calibration.result().unwrap();
        assert!((left - 28).abs() <= 3, "{left}");
        assert!((right - 11).abs() <= 3, "{right}");
    }

    #[test]
    fn stray_touches_and_silence_give_no_correction() {
        let now = Instant::now();
        let mut calibration = Calibration::new(now);
        for _ in 0..Calibration::steps() {
            tap(&mut calibration, 300, now);
        }
        assert_eq!(calibration.result(), None);
        assert!(!calibration.timed_out(now + Duration::from_secs(29)));
        assert!(calibration.timed_out(now + Duration::from_secs(30)));
        // A lift without a touch down does not count.
        let mut calibration = Calibration::new(now);
        calibration.touch(TouchPhase::Ended, 40, now);
        assert_eq!(calibration.step(), 0);
    }
}
