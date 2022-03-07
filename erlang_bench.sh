#!/usr/bin/env bash

set -eo pipefail

REPO_URL="https://github.com/ergl/lasp-bench.git"

do_download() {
    local folder="${1}"
    git clone "${REPO_URL}" --single-branch --branch bench_ext "${folder}"
}

do_compile() {
    local home_directory="${1}"
    pushd "${home_directory}/sources/lasp-bench"
    ./rebar3 compile
    ./rebar3 escriptize
    popd
}

do_load_ext() {
    local home_directory="${1}"

    local master_node="${2}"
    local master_port="${3}"

    local target_replica="${4}"
    local key_number="${5}"
    local value_bytes="${6}"

    pushd "${home_directory}/sources/lasp-bench/scripts"
    ./ext_load.escript \
        --master_ip "${master_node}" \
        --master_port "${master_port}" \
        --replica "${target_replica}" \
        --keys "${key_number}" \
        --value_bytes "${value_bytes}"
    popd
}

do_rebuild() {
    local home_directory="${1}"
    pushd "${home_directory}/sources/lasp-bench"
    git fetch origin
    git reset --hard origin/bench_ext
    popd
}

do_compress() {
    local home_directory="${1}"
    local target
    target=$(readlink -f "${home_directory}/sources/lasp-bench/tests/current")
    target=$(basename "${target}")
    pushd "${home_directory}/sources/lasp-bench/tests/"
    tar -czf "${home_directory}/results.tar.gz" "${target}"
    popd
}

do_run() {
    local home_directory="${1}"
    local replica="${2}"
    local node="${3}"
    local port="${4}"
    local config="${5}"
    pushd "${home_directory}/sources/lasp-bench"
    (
        export REPLICA_NAME="${replica}"; export MASTER_NODE="${node}"; export MASTER_PORT="${port}"; ./_build/default/bin/lasp_bench "${config}"
    )
    popd
}

usage() {
    echo "erlang_bench.sh [-h] [-H <home>] download | compile | load_ext <master-node> <master-port> <replica> <keys> <value_bytes> | run <config> <master-node> <master-port> <replica> | rebuild | compress"
}

run () {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    local home_directory="${HOME}"
    while getopts ":yH:h" opt; do
        case $opt in
            h)
                usage
                exit 0
                ;;
            H)
                home_directory="${OPTARG}"
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
            rm -rf "${home_directory}/sources/lasp-bench"
            do_download "${home_directory}/sources/lasp-bench"
            ;;
        "compile")
            do_compile "${home_directory}"
            exit $?
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

            echo -e "Loading with ${config_file}\n"
            do_load_ext "${home_directory}" \
                "${master_node}" \
                "${master_port}" \
                "${bench_replica}" \
                "${key_number}" \
                "${value_bytes}"
            ;;
        "run")
            if [[ $# -ne 5 ]]; then
                usage
                exit 1
            fi
            local run_config_file="${2}"
            local master_node="${3}"
            local master_port="${4:-7087}"
            local bench_replica="${5}"

            echo -e "Runnig with ${run_config_file}\n"
            do_run "${home_directory}" "${bench_replica}" "${master_node}" "${master_port}" "${run_config_file}"
            exit $?
            ;;
        "rebuild")
            do_rebuild "${home_directory}"
            do_compile "${home_directory}"
            exit $?
            ;;
        "compress")
            do_compress "${home_directory}"
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
