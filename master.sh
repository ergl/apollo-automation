#!/usr/bin/env bash

set -eo pipefail

BIN_NAME=master_linux_amd64

usage() {
    echo "master.sh [-h] download <token> <tag> | run <leader-replica> <replicas> <partitions> <tag> | stop"
}

do_download() {
    local token="${1}"
    local release_tag="${2}"
    local folder="${3}"
    GITHUB_API_TOKEN=${token} ./fetch_gh_release.sh -t "${release_tag}" -f "${BIN_NAME}"
    chmod u+x "${BIN_NAME}"
    mkdir -p "${folder}/${release_tag}"
    mv "${BIN_NAME}" "${folder}/${release_tag}"
}

do_run() {
    local leader="${1}"
    local replicas="${2}"
    local partitions="${3}"
    local tag="${4}"
    screen -dmSL \
        "${BIN_NAME}" \
        "${HOME}"/sources/${tag}/${BIN_NAME} -leader "${leader}" -n "${replicas}" -p "${partitions}"
}

run () {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    while getopts ":h" opt; do
        case $opt in
            h)
                usage
                exit 0
                ;;
            *)
                echo "Unrecognized option -${OPTARG}"
                usage
                exit 1
                ;;
        esac
    done

    shift $((OPTIND - 1))

    if [[ $# -lt 1 ]]; then
        usage
        exit 1
    fi

    local command="${1}"
    case $command in
        "download")
            local token="${2}"
            local tag="${3}"
            do_download "${token}" "${tag}" "${HOME}/sources/"
            ;;
        "run")
            local leader_replica="${2}"
            local replicas="${3}"
            local partitions="${4}"
            local tag="${5}"
            echo -e "Runnig master with leader ${leader_replica} (${partitions} partitions)\n"
            do_run "${leader_replica}" "${replicas}" "${partitions}" "${tag}"
            exit $?
            ;;
        "stop")
            screen_name=$(screen -ls | grep -o -P "\d+.${BIN_NAME}")
            screen -X -S "${screen_name}" quit
            exit $?
            ;;
        *)
            echo "Unrecognized command ${command}"
            usage
            exit 1
            ;;
    esac
}

run "$@"
