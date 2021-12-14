#!/usr/bin/env bash

set -eo pipefail

usage() {
    echo "measure_cpu.sh [-h] -f output_file"
}

run () {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    local output
    local now
    while getopts ":f:h" opt; do
        case $opt in
            h)
                usage
                exit 0
                ;;
            f)
                output="${OPTARG}"
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

    if [[ -z "${output}" ]]; then
        echo "Output file path can't be empty string."
        usage
        exit 1
    fi

    local current_time
    date --iso-8601=ns > "${output}"
    # This results in a csv file with `cpu_number,load`, where load is defined as 100 - (idle %)
    mpstat -P ALL 5 1 | grep Average | grep -v CPU | awk '
    BEGIN { max_nr=0 }
    {
        if ((NR-1) > max_nr) { max_nr = NR - 1 }
        a[NR-1, 0] = $2
        a[NR-1, 1] = 100-$NF
    }
    END {
        print "cpu,load"
        for (i=0; i <= max_nr; i++) {
            printf("%s,%s\n", a[i,0], a[i,1])
        }
    }
    ' >> "${output}"
}

run "$@"
