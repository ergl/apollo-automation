#!/usr/bin/env bash

set -eo pipefail

run() {
    if [[ $# -ne 1 ]]; then
        exit 1
    fi

    local target
    local dir
    local base

    target="${1}"
    dir=$(dirname "${target}")
    base=$(basename "${target}")

    pushd "${dir}" >/dev/null
    tar -czf "${base}.tar.gz" "${base}"
    popd >/dev/null
}

run "$@"
