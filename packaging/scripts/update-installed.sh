#!/usr/bin/env bash
# Builds this checkout and replaces the binaries of an installed Linexus
# package (.rpm, .deb or Arch) in /usr, then restarts the running services.
# The next package update puts the packaged binaries back.
#   update-installed.sh            build, install, restart
#   update-installed.sh --no-build install the last release build again
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
bin_dir="$repo_root/target/release"
build=1

case "${1:-}" in
  "") ;;
  --no-build) build=0 ;;
  -h|--help) sed -n '2,6p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  *) echo "unknown option: $1" >&2; exit 2 ;;
esac

if [[ ! -x /usr/bin/linexusd ]]; then
  echo "No packaged Linexus in /usr/bin. Use packaging/scripts/install-dev.sh instead." >&2
  exit 1
fi

if [[ $build -eq 1 ]]; then
  echo "==> Building (release)"
  (cd "$repo_root" && cargo build --release --locked \
    -p nexusctl -p nexus-daemon -p nexus-inputd -p nexus-studio)
fi

for bin in linexusctl linexusd linexus-studio linexus-inputd; do
  [[ -x "$bin_dir/$bin" ]] || { echo "missing $bin_dir/$bin" >&2; exit 1; }
done

echo "==> Installing to /usr (sudo)"
sudo install -m755 "$bin_dir/linexusctl" "$bin_dir/linexusd" "$bin_dir/linexus-studio" /usr/bin/
sudo install -Dm755 "$bin_dir/linexus-inputd" /usr/lib/linexus/linexus-inputd

echo "==> Restarting services"
for unit in linexusd linexus-inputd; do
  if systemctl --user is-active --quiet "$unit"; then
    systemctl --user restart "$unit" && echo "    $unit restarted"
  fi
done
while read -r unit; do
  [[ -n "$unit" ]] && sudo systemctl restart "$unit" && echo "    $unit restarted"
done < <(systemctl list-units --state=active --plain --no-legend 'linexus-inputd@*' | awk '{print $1}')

if pkill -x linexus-studio; then
  echo "    Studio was open: it is closed, start it again to get the new version"
fi

echo "==> Done: $(/usr/bin/linexusctl --version 2>/dev/null || echo installed)"
