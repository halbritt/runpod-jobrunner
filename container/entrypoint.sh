#!/usr/bin/env bash
set -euo pipefail

umask 077

readonly request_path="${RUNPOD_JOBRUNNER_REQUEST_PATH:-/workspace/runpod-jobrunner/request.json}"
readonly status_dir="${RUNPOD_JOBRUNNER_STATUS_DIR:-/workspace/runpod-jobrunner/status}"
readonly token_file="${RUNPOD_JOBRUNNER_TOKEN_FILE:-/workspace/runpod-jobrunner/status-token}"
readonly wait_seconds="${RUNPOD_JOBRUNNER_REQUEST_WAIT_SECONDS:-600}"
readonly retention_seconds="${RUNPOD_JOBRUNNER_STATUS_RETENTION_SECONDS:-3600}"

case "${wait_seconds}" in
    ''|*[!0-9]*)
        echo "RUNPOD_JOBRUNNER_REQUEST_WAIT_SECONDS must be a positive integer" >&2
        exit 64
        ;;
esac
case "${retention_seconds}" in
    ''|*[!0-9]*)
        echo "RUNPOD_JOBRUNNER_STATUS_RETENTION_SECONDS must be a nonnegative integer" >&2
        exit 64
        ;;
esac
if (( wait_seconds == 0 )); then
    echo "RUNPOD_JOBRUNNER_REQUEST_WAIT_SECONDS must be greater than zero" >&2
    exit 64
fi

for durable_path in "${request_path}" "${status_dir}" "${token_file}"; do
    case "${durable_path}" in
        /workspace/*) ;;
        *)
            echo "runner state path must be below the encrypted /workspace mount" >&2
            exit 64
            ;;
    esac
done

if [[ ! -x /start.sh ]]; then
    echo "RunPod SSH bootstrap /start.sh is unavailable" >&2
    exit 69
fi

# The official RunPod bootstrap owns SSH setup. tini remains PID 1 and adopts
# both the bootstrap descendants and the remote runner.
/start.sh &

readonly deadline=$((SECONDS + wait_seconds))
until [[ -s "${request_path}" && -s "${token_file}" ]]; do
    if (( SECONDS >= deadline )); then
        echo "timed out waiting for the run request and status token" >&2
        exit 124
    fi
    sleep 0.25
done

if [[ -L "${request_path}" || -L "${token_file}" ]]; then
    echo "run request and status token must not be symbolic links" >&2
    exit 65
fi

mkdir -p -- "${status_dir}"
exec runpod-jobrunner-remote \
    "${request_path}" \
    "${status_dir}" \
    "${token_file}" \
    --status-host 0.0.0.0 \
    --status-port 8080 \
    --status-retention-seconds "${retention_seconds}"
