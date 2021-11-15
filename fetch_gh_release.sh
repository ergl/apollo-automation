#!/usr/bin/env bash

# https://stackoverflow.com/a/35688093

set -eo pipefail

REPO_URL="https://api.github.com/repos/ergl/unistore-ext"
RELEASE_URL="${REPO_URL}/releases/tags"
ASSET_URL="${REPO_URL}/releases/assets"
AUTH="Authorization: token ${GITHUB_API_TOKEN}"

usage() {
    echo -e "GITHUB_API_TOKEN=\"XXX\" fetch_gh_release.sh [-h] [-t tag_name] [-f asset_name]"
}

run() {
    if [[ $# -eq 0 ]]; then
        usage
        exit 1
    fi

    local tag_name=""
    local asset_name=""
    while getopts ":ht:f:" opt; do
        case $opt in
            t)
                tag_name="${OPTARG}"
                ;;
            f)
                asset_name="${OPTARG}"
                ;;
            h)
                usage
                exit 0
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

    # Validate token
    [ "$GITHUB_API_TOKEN" ] || { echo "Error: Please define GITHUB_API_TOKEN." >&2; exit 1; }

    curl -o /dev/null -sH "${AUTH}" "${REPO_URL}" || { echo "Error: Invalid repo, token, or network issue."; exit 1; }
    response=$(curl -sH "${AUTH}" "${RELEASE_URL}/${tag_name}")
    asset_id=$(echo "${response}" | jq --arg asset_name "${asset_name}" '.assets[] | select(.name == $asset_name).id')
    [ "${asset_id}" ] || { echo -e "Error: Failed to get asset id for asset ${asset_name}, response:\n${response}"; exit 1; }
    
    echo "Downloading ${asset_name}"
    curl -L -o ${asset_name} -H "${AUTH}" -H 'Accept: application/octet-stream' "${ASSET_URL}/${asset_id}"
}

run "$@"
