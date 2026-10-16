#!/usr/bin/env bash
set -euo pipefail

runtime_backend="${1:-both}"
if [[ $# -gt 0 ]]; then
    shift
fi
case "$runtime_backend" in
    docker|microsandbox|both) ;;
    *) echo "Usage: $0 [docker|microsandbox|both] [Maven arguments...]" >&2; exit 2 ;;
esac
if [[ "$runtime_backend" != docker ]]; then
    : "${FLOCI_TEST_MICROSANDBOX_URL:?Set the native helper URL before running the microsandbox contract}"
    : "${FLOCI_TEST_MICROSANDBOX_TOKEN:?Set the native helper token before running the microsandbox contract}"
fi

runtime_repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$runtime_repo"
# Limit discovery too: Quarkus otherwise augments unrelated test profiles before tag filtering.
runtime_tests="$(
    find src/test/java -type f -name '*.java' -print | LC_ALL=C sort |
    while IFS= read -r runtime_source; do
        if grep -Eq '^[[:space:]]*@Tag[[:space:]]*\([[:space:]]*"runtime"[[:space:]]*\)' "$runtime_source"; then
            runtime_class="${runtime_source#src/test/java/}"
            runtime_class="${runtime_class%.java}"
            printf '%s\n' "${runtime_class//\//.}"
        fi
    done | paste -sd ',' -
)"
if [[ -z "$runtime_tests" ]]; then
    echo 'No shared tests with @Tag("runtime") found in src/test/java' >&2
    exit 1
fi
run_backend() {
    local selected="$1"
    shift
    echo "Running shared AWS runtime contracts against $selected"
    FLOCI_TEST_RUNTIME="$selected" ./mvnw test "-Dtest=$runtime_tests" "$@" -Dgroups=runtime
}
if [[ "$runtime_backend" == both ]]; then
    run_backend docker "$@"
    run_backend microsandbox "$@"
else
    run_backend "$runtime_backend" "$@"
fi
