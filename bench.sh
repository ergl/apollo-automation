#!/usr/bin/env bash

set -eo pipefail

REPO_URL="https://github.com/ergl/lasp-bench.git"

do_download() {
    local branch="${1}"
    local folder="${2}"
    git clone "${REPO_URL}" --single-branch --branch "${branch}" "${folder}"
}

do_compile() {
    local profile="${1}"
    pushd "${HOME}/sources/lasp-bench"
    ./rebar3 as "${profile}" compile
    ./rebar3 as "${profile}" escriptize
    popd
}

do_load_ext() {
    local confirm_load="${1}"
    local target_machine="${2}"
    local target_port="${3}"
    local target_replica="${4}"
    local config_file="${5}"

    if [[ "${confirm_load}" -eq 1 ]]; then
        pushd "${HOME}/sources/lasp-bench/scripts"
        ./bench_load.escript \
            -a "${target_machine}" \
            -p "${target_port}" \
            -r "${target_replica}" \
            -c ext="${config_file}"
        popd
    else
        read -r -n 1 -p "Load target ${target_machine}:${target_port} ? [y/n] " response
        case "${response}" in
            [yY] )
                pushd "${HOME}/sources/lasp-bench/scripts"
                ./bench_load.escript \
                    -a "${target_machine}" \
                    -p "${target_port}" \
                    -r "${target_replica}" \
                    -c ext="${config_file}"
                popd
                ;;
            *)
                echo -e "\\nLoad aborted"
                ;;
        esac
    fi
}

do_rebuild() {
    local branch="${1}"
    pushd "${HOME}/sources/lasp-bench"
    git fetch origin
    git reset --hard origin/"${branch}"
    popd
}

do_compress() {
    local target
    target=$(readlink -f "${HOME}/sources/lasp-bench/tests/current")
    target=$(basename "${target}")
    pushd "${HOME}/sources/lasp-bench/tests/"
    tar -czf "${HOME}/results.tar.gz" "${target}"
    popd
}

do_run() {
    local profile="${1}"
    local replica="${2}"
    local node="${3}"
    local port="${4}"
    local config="${5}"
    pushd "${HOME}/sources/lasp-bench"
    (
        export REPLICA_NAME="${replica}"; export MASTER_NODE="${node}"; export MASTER_PORT="${port}"; ./_build/"${profile}"/bin/lasp_bench "${config}"
    )
    popd
}

usage() {
    echo "bench.sh [-hy] [-b <branch>=bench_ext] [-p <profile>=default] download | compile | load_ext <master-node> <master-port> <replica> <config> | run <config> <master-node> <master-port> <replica> | rebuild | compress"
}

run () {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    local branch="bench_ext"
    local profile="default"
    local confirm_load=0
    while getopts ":yb:p:h" opt; do
        case $opt in
            h)
                usage
                exit 0
                ;;
            b)
                branch="${OPTARG}"
                ;;
            p)
                profile="${OPTARG}"
                ;;
            y)
                confirm_load=1
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
            do_download "${branch}" "${HOME}/sources/lasp-bench"
            ;;
        "compile")
            do_compile "${profile}"
            exit $?
            ;;
        "load_ext")
            if [[ $# -ne 5 ]]; then
                usage
                exit 1
            fi
            local master_node="${2}"
            local master_port="${3:-7087}"
            local bench_replica="${4}"
            local config_file="${5}"
            echo -e "Loading with ${config_file}\n"
            do_load_ext "${confirm_load}" "${master_node}" "${master_port}" "${bench_replica}" "${config_file}"
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
            do_run "${profile}" "${bench_replica}" "${master_node}" "${master_port}" "${run_config_file}"
            exit $?
            ;;
        "rebuild")
            do_rebuild "${branch}"
            do_compile "${profile}"
            exit $?
            ;;
        "compress")
            do_compress
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
