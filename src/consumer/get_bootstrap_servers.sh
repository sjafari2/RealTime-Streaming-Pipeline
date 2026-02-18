#!/usr/bin/env bash
set -euo pipefail

CONFIGMAP_PATH="${CONFIGMAP_PATH:-/config/pipeline-configmap.yaml}"

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf "%s" "$s"
}

get_cfg() {
  local key="$1"
  local default="${2:-}"
  local val=""

  if [[ -f "${CONFIGMAP_PATH}" ]]; then
    val="$(awk -v k="$key" '
      BEGIN { in_data=0 }
      /^[[:space:]]*data:[[:space:]]*$/ { in_data=1; next }
      in_data==1 && /^[^[:space:]].*:[[:space:]]*$/ { exit }
      in_data==1 {
        if ($0 ~ "^[[:space:]]+" k ":[[:space:]]*") {
          sub("^[[:space:]]+" k ":[[:space:]]*", "", $0)
          print $0
          exit
        }
      }
    ' "${CONFIGMAP_PATH}" 2>/dev/null || true)"
  fi

  val="$(trim "${val}")"
  if [[ "${val}" =~ ^\".*\"$ ]]; then val="${val:1:${#val}-2}"; fi
  if [[ "${val}" =~ ^\'.*\'$ ]]; then val="${val:1:${#val}-2}"; fi

  if [[ -n "${val}" ]]; then
    printf "%s" "${val}"
  else
    printf "%s" "${default}"
  fi
}

# You can store bootstrap servers either as BOOTSTRAP_SERVERS or KAFKA_BOOTSTRAP_SERVERS in the ConfigMap.
bs="$(get_cfg BOOTSTRAP_SERVERS "")"
if [[ -z "$bs" ]]; then
  bs="$(get_cfg KAFKA_BOOTSTRAP_SERVERS "")"
fi

if [[ -z "$bs" ]]; then
  echo "[ERROR] BOOTSTRAP_SERVERS not found in ${CONFIGMAP_PATH} (.data.BOOTSTRAP_SERVERS)" >&2
  exit 1
fi

echo -n "$bs"

