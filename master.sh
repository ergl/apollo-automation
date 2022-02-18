#!/usr/bin/env bash

set -eo pipefail

BIN_NAME=master_linux_amd64

usage() {
    echo "master.sh [-h] [-T tag] download <token> | run <replicas> <partitions> [arguments ...] | stop"
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

run () {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    local tag
    while getopts ":hT:" opt; do
        case $opt in
            h)
                usage
                exit 0
                ;;
            T)
                tag="${OPTARG}"
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
            do_download "${token}" "${tag}" "${HOME}/sources/"
            ;;
        "run")
            # Remove "run"
            shift
            local replicas="${1}"
            shift
            local partitions="${1}"
            shift
            echo -e "Runnig master with ${partitions} partitions and ${replicas} replicas\n"

            screen -dmSL \
                "${BIN_NAME}" \
                "${HOME}"/sources/${tag}/${BIN_NAME} -n "${replicas}" -p "${partitions}" "${@}"

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
