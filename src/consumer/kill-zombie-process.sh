#!/usr/bin/env bash
set -euo pipefail

user=${1:-$USER}

echo "Scanning for zombie (<defunct>) processes owned by: $user"
# Collect unique parent PIDs of zombies for this user
mapfile -t PPIDS < <(ps -eo stat,ppid,user,pid,cmd --no-headers \
  | awk -v u="$user" '$1 ~ /^Z/ && $3 == u {print $2}' | sort -u)

if [[ ${#PPIDS[@]} -eq 0 ]]; then
  echo "No zombies found for $user."
  exit 0
fi

echo "Found parent PIDs: ${PPIDS[*]}"
echo

for ppid in "${PPIDS[@]}"; do
  # Skip if parent already gone
  if [[ ! -d "/proc/$ppid" ]]; then
    echo "Parent $ppid already exited; its zombies should vanish shortly."
    continue
  fi

  # Parent info
  parent_line=$(ps -o pid,ppid,user,stat,etime,cmd -p "$ppid" --no-headers || true)
  parent_state=$(awk '/^State:/{print $2}' "/proc/$ppid/status" 2>/dev/null || echo "?")

  echo "Parent: $parent_line"

  # Show its zombies
  echo "  Children in Z state:"
  ps --ppid "$ppid" -o pid,stat,etime,cmd --no-headers | awk '$2 ~ /^Z/ {print "   - "$0}'
  echo

  # If parent is PID 1, we can't kill it; zombies should be reaped by init/systemd
  if [[ "$ppid" -eq 1 ]]; then
    echo "  NOTE: Parent is PID 1. You cannot kill PID 1. If these persist, it may indicate a kernel/FS issue; a reboot often clears them."
    echo
    continue
  fi

  # If parent is in uninterruptible sleep (D), signals won't take effect until I/O returns
  if [[ "$parent_state" == "D" ]]; then
    echo "  WARNING: Parent is in uninterruptible I/O (state D). Signals may not work now. Check stuck mounts/disks; a reboot may be required."
  fi

  # Nudge with SIGCHLD first (often enough if the parent forgot to wait)
  echo "  -> Sending SIGCHLD to $ppid"
  kill -s CHLD "$ppid" 2>/dev/null || true
  sleep 0.5

  # Re-check if any Z children remain
  if ps --ppid "$ppid" -o stat --no-headers | grep -q '^Z'; then
    echo "  -> Sending SIGTERM to $ppid"
    kill -TERM "$ppid" 2>/dev/null || true
    sleep 1
  fi

  if ps --ppid "$ppid" -o stat --no-headers | grep -q '^Z' && [[ -d "/proc/$ppid" ]]; then
    echo "  -> Sending SIGKILL to $ppid"
    kill -KILL "$ppid" 2>/dev/null || true
    sleep 1
  fi

  # Final status
  if ps --ppid "$ppid" -o stat --no-headers | grep -q '^Z'; then
    echo "  ❗ Zombies still present under parent $ppid. If parent process persists or is in state D, investigate I/O/mounts or consider reboot."
  else
    echo "  ✅ Zombies under parent $ppid should be gone."
  fi

  echo
done

echo "Done."

