#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# create_topics.sh
#
# Desired format (Mountain Time):
#   RUN_ID     = "run-YYYYMMDD-HHMMSS"
#   TOPIC_TITLE= "<EXP_ID>-R<TARGET_RATE>-YYYYMMDD-HHMMSS"
#
# Topics created:
#   <TOPIC_TITLE>_<i> for i in [0..TOPIC_COUNT-1]
#
# No yq/perl; edits the YAML using awk. CONFIGMAP_PATH must be writable.
# -----------------------------------------------------------------------------

CONFIGMAP_PATH="${CONFIGMAP_PATH:-/config/pipeline-configmap.yaml}"

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf "%s" "$s"
}

# Read KEY from flat ConfigMap YAML under .data
get_cfg() {
  local key="$1"
  local default="${2:-}"
  local val=""

  if [[ -f "${CONFIGMAP_PATH}" ]]; then
    val="$(awk -v k="$key" '
      BEGIN { in_data=0 }
      /^[[:space:]]*data:[[:space:]]*$/ { in_data=1; next }
      in_data==1 && /^[^[:space:]].*:[[:space:]]*$/ { exit }  # next top-level key
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

# Update or insert KEY: "VALUE" under the data: block.
set_cfg() {
  local key="$1"
  local value="$2"

  if [[ ! -f "${CONFIGMAP_PATH}" ]]; then
    echo "[ERROR] Config file not found: ${CONFIGMAP_PATH}" >&2
    exit 1
  fi
  if [[ ! -w "${CONFIGMAP_PATH}" ]]; then
    echo "[ERROR] Config file is not writable: ${CONFIGMAP_PATH}" >&2
    echo "If this is a ConfigMap mount, it is read-only. Use a writable shared file (PVC/emptyDir)." >&2
    exit 1
  fi

  local tmp
  tmp="$(mktemp)"

  awk -v k="${key}" -v v="${value}" '
    function print_kv() { printf "  %s: \"%s\"\n", k, v }
    BEGIN { in_data=0; seen_data=0; updated=0 }

    /^[[:space:]]*data:[[:space:]]*$/ {
      in_data=1; seen_data=1
      print $0
      next
    }

    in_data==1 && /^[^[:space:]].*:[[:space:]]*$/ {
      if (updated==0) { print_kv(); updated=1 }
      in_data=0
      print $0
      next
    }

    in_data==1 && $0 ~ "^[[:space:]]+" k ":[[:space:]]*" {
      print_kv()
      updated=1
      next
    }

    { print $0 }

    END {
      if (seen_data==0) {
        print ""
        print "data:"
        print_kv()
      } else if (in_data==1 && updated==0) {
        print_kv()
      }
    }
  ' "${CONFIGMAP_PATH}" > "${tmp}"

  mv "${tmp}" "${CONFIGMAP_PATH}"
}

locate_kafka_topics() {
  local kt="${KAFKA_TOPICS:-}"
  if [[ -n "${kt}" && -x "${kt}" ]]; then echo -n "${kt}"; return 0; fi
  if command -v kafka-topics.sh >/dev/null 2>&1; then echo -n "$(command -v kafka-topics.sh)"; return 0; fi
  for p in "/kafka/bin/kafka-topics.sh" "/opt/bitnami/kafka/bin/kafka-topics.sh"; do
    if [[ -x "$p" ]]; then echo -n "$p"; return 0; fi
  done
  return 1
}

supports_if_not_exists() {
  local kt="$1"
  "${kt}" --help 2>&1 | grep -q -- '--if-not-exists'
}

# --------------------------- read config --------------------------------------

BOOTSTRAP_SERVERS="$(get_cfg BOOTSTRAP_SERVERS "")"
TOPIC_COUNT="$(get_cfg TOPIC_COUNT 1)"
NUM_PARTITIONS="$(get_cfg NUM_PARTITIONS 36)"
REPLICATION_FACTOR="$(get_cfg REPLICATION_FACTOR 1)"
RETENTION_MS="$(get_cfg RETENTION_MS 1800000)"
SEGMENT_MS="$(get_cfg SEGMENT_MS 600000)"
CLEANUP_POLICY="$(get_cfg CLEANUP_POLICY delete)"

EXP_ID="$(get_cfg EXP_ID "")"
TARGET_RATE="$(get_cfg TARGET_RATE "")"

if [[ -z "${BOOTSTRAP_SERVERS}" ]]; then
  echo "[ERROR] BOOTSTRAP_SERVERS missing in .data.BOOTSTRAP_SERVERS" >&2
  exit 3
fi
if [[ -z "${EXP_ID}" ]]; then
  echo "[ERROR] EXP_ID missing in .data.EXP_ID (e.g., B0)" >&2
  exit 4
fi
if [[ -z "${TARGET_RATE}" ]]; then
  echo "[ERROR] TARGET_RATE missing in .data.TARGET_RATE (e.g., 500)" >&2
  exit 5
fi

for v in TOPIC_COUNT NUM_PARTITIONS REPLICATION_FACTOR; do
  eval "x=\${$v}"
  if ! [[ "${x}" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] ${v} must be integer; got '${x}'" >&2
    exit 2
  fi
done
if ! [[ "${TARGET_RATE}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "[ERROR] TARGET_RATE must be a nonnegative decimal; got '${TARGET_RATE}'" >&2
  exit 2
fi

# --------------------------- generate ids (Mountain Time) ----------------------

RUN_TS="${RUN_TS:-$(TZ=America/Denver date +"%Y%m%d-%H%M%S")}"
RUN_ID="run-${RUN_TS}"
TRAFFIC_MODE="$(get_cfg TRAFFIC_MODE balanced)"
TOPIC_TITLE="${EXP_ID}-${TRAFFIC_MODE}-R${TARGET_RATE}-${RUN_TS}"

# --------------------------- kafka-topics.sh ----------------------------------

KAFKA_TOPICS="$(locate_kafka_topics || true)"
if [[ -z "${KAFKA_TOPICS}" ]]; then
  echo "[ERROR] kafka-topics.sh not found." >&2
  exit 1
fi

IF_NOT_EXISTS_FLAG=""
if supports_if_not_exists "${KAFKA_TOPICS}"; then
  IF_NOT_EXISTS_FLAG="--if-not-exists"
fi

echo "[INFO] Using kafka-topics at: ${KAFKA_TOPICS}"
echo "[INFO] Bootstrap servers: ${BOOTSTRAP_SERVERS}"
echo "[INFO] RUN_ID=${RUN_ID}"
echo "[INFO] TOPIC_TITLE=${TOPIC_TITLE}"
echo "[INFO] TOPIC_COUNT=${TOPIC_COUNT} PARTITIONS=${NUM_PARTITIONS} RF=${REPLICATION_FACTOR}"

# --------------------------- create topics ------------------------------------

for i in $(seq 0 $((TOPIC_COUNT - 1))); do
  topic="${TOPIC_TITLE}_${i}"
  echo "[INFO] Creating topic: ${topic}"
  set +e
  "${KAFKA_TOPICS}" --create \
    ${IF_NOT_EXISTS_FLAG} \
    --bootstrap-server "${BOOTSTRAP_SERVERS}" \
    --topic "${topic}" \
    --partitions "${NUM_PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --config retention.ms="${RETENTION_MS}" \
    --config segment.ms="${SEGMENT_MS}" \
    --config cleanup.policy="${CLEANUP_POLICY}"
  rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "[ERROR] Topic creation failed: ${topic}" >&2
    exit "$rc"
  fi
done

# --------------------------- write back ---------------------------------------

set_cfg "RUN_ID" "${RUN_ID}"
set_cfg "TOPIC_TITLE" "${TOPIC_TITLE}"

echo "[SUCCESS] Topics created with prefix: ${TOPIC_TITLE}"
echo "[SUCCESS] Updated ${CONFIGMAP_PATH}: RUN_ID, TOPIC_TITLE"

