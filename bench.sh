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
    local home_directory="${2}"
    pushd "${home_directory}/sources/lasp-bench"
    ./rebar3 as "${profile}" compile
    ./rebar3 as "${profile}" escriptize
    popd
}

do_load_ext() {
    local home_directory="${1}"
    local confirm_load="${2}"
    local target_machine="${3}"
    local target_port="${4}"
    local target_replica="${5}"
    local config_file="${6}"


    if [[ "${confirm_load}" -eq 1 ]]; then
        pushd "${home_directory}/sources/lasp-bench/scripts"
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
                pushd "${home_directory}/sources/lasp-bench/scripts"
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
    local home_directory="${2}"
    pushd "${home_directory}/sources/lasp-bench"
    git fetch origin
    git reset --hard origin/"${branch}"
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
    local profile="${2}"
    local replica="${3}"
    local node="${4}"
    local port="${5}"
    local config="${6}"
    pushd "${home_directory}/sources/lasp-bench"
    (
        export REPLICA_NAME="${replica}"; export MASTER_NODE="${node}"; export MASTER_PORT="${port}"; ./_build/"${profile}"/bin/lasp_bench "${config}"
    )
    popd
}

usage() {
    echo "bench.sh [-hy] [-b <branch>=bench_ext] [-p <profile>=default] [-H <home>] download | compile | load_ext <master-node> <master-port> <replica> <config> | run <config> <master-node> <master-port> <replica> | rebuild | compress"
}

run () {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    local branch="bench_ext"
    local profile="default"
    local home_directory="${HOME}"
    local confirm_load=0
    while getopts ":yb:p:H:h" opt; do
        case $opt in
            h)
                usage
                exit 0
                ;;
            H)
                home_directory="${OPTARG}"
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
            rm -rf "${home_directory}/sources/lasp-bench"
            do_download "${branch}" "${home_directory}/sources/lasp-bench"
            ;;
        "compile")
            do_compile "${profile}" "${home_directory}"
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
            do_load_ext "${home_directory}" "${confirm_load}" "${master_node}" "${master_port}" "${bench_replica}" "${config_file}"
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
            do_run "${home_directory}" "${profile}" "${bench_replica}" "${master_node}" "${master_port}" "${run_config_file}"
            exit $?
            ;;
        "rebuild")
            do_rebuild "${branch}" "${home_directory}"
            do_compile "${profile}" "${home_directory}"
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
