#!/usr/bin/env bash

set -eo pipefail

RUNNER_BIN_NAME=runner_linux_amd64
LOAD_BIN_NAME=load_linux_amd64

do_download() {
    local home_directory="${1}"
    local token="${2}"
    local release_tag="${3}"
    local folder="${4}"

    pushd "${home_directory}"

    mkdir -p "${folder}/${release_tag}"

    GITHUB_API_TOKEN=${token} ./fetch_gh_release.sh -t "${release_tag}" -f "${RUNNER_BIN_NAME}"
    chmod u+x "${RUNNER_BIN_NAME}"
    mv "${RUNNER_BIN_NAME}" "${folder}/${release_tag}"

    GITHUB_API_TOKEN=${token} ./fetch_gh_release.sh -t "${release_tag}" -f "${LOAD_BIN_NAME}"
    chmod u+x "${LOAD_BIN_NAME}"
    mv "${LOAD_BIN_NAME}" "${folder}/${release_tag}"

    popd
}

do_load_ext() {
    local home_directory="${1}"

    local master_node="${2}"
    local master_port="${3}"

    local target_replica="${4}"
    local key_number="${5}"
    local value_bytes="${6}"

    "${home_directory}"/sources/${tag}/${LOAD_BIN_NAME} \
        -replica "${target_replica}" \
        -master_ip "${master_node}" \
        -master_port "${master_port}" \
        -keys "${key_number}" \
        -value_bytes "${value_bytes}"
}

do_compress() {
    local home_directory="${1}"
    local result_path="${2}"

    local target_dir
    local target_folder
    target_dir=$(dirname "${result_path}")
    target_folder=$(basename "${result_path}")

    pushd "${target_dir}"
    tar -czf "${home_directory}/results.tar.gz" "${target_folder}"
    popd
}

usage() {
    echo "bench.sh [-h] [-H <home>] [-T tag] download <token> | load_ext <master-node> <master-port> <replica> <keys> <value_bytes> | run <argument-string> | compress <path>"
}

run () {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    local home_directory="${HOME}"
    local tag
    while getopts ":yT:H:h" opt; do
        case $opt in
            h)
                usage
                exit 0
                ;;
            H)
                home_directory="${OPTARG}"
                ;;
            T)
                tag="${OPTARG}"
                ;;
            :)
                echo "Option -${OPTARG} requires an argument"
                usage
                exit 1
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
            do_download "${home_directory}" "${token}" "${tag}" "${home_directory}/sources/"
            ;;

        "load_ext")
            if [[ $# -ne 6 ]]; then
                usage
                exit 1
            fi
            local master_node="${2}"
            local master_port="${3:-7087}"
            local bench_replica="${4}"
            local key_number="${5}"
            local value_bytes="${6}"

            echo -e "Loading ${bench_replica} with ${key_number} keys\n"
            do_load_ext "${home_directory}" \
                "${master_node}" \
                "${master_port}" \
                "${bench_replica}" \
                "${key_number}" \
                "${value_bytes}"
            ;;

        "run")
            # Remove "run"
            shift
            echo -e "Running benchmark\n"
            "${home_directory}"/sources/"${tag}"/"${RUNNER_BIN_NAME}" "${@}"
            exit $?
            ;;

        "compress")
            if [[ $# -ne 2 ]]; then
                usage
                exit 1
            fi
            local result_path="${2}"
            do_compress "${home_directory}" "${result_path}"
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
