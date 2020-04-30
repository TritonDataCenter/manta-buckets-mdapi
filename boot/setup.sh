#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
PG_USER=buckets_mdapi
SVC_ROOT=/opt/smartdc/buckets-mdapi
ZONE_UUID=$(/usr/bin/zonename)
SAPI_CONFIG=
SAPI_URL=$(mdata-get SAPI_URL)
[[ -n $SAPI_URL ]] || fatal "no SAPI_URL found"
#
# Wait 10 seconds for dns to become operational. This is displeasing, but
# otherwise we'll encounter dns resolution errors. This logic is lifted from
# electric-moray.
#
sleep 10

export PATH=$SVC_ROOT/bin:$SVC_ROOT/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH

function get_sapi_config {
    local sapi_res
    local err
    #
    # Load the zone's full config object from SAPI. If this request fails, it
    # will be retried until the smf(5) method timeout expires for the
    # "mdata:execute" service.
    #
    # We do this because we need to access SAPI config variables in this script,
    # and it's more difficult to get them from the buckets-mdapi config toml file.
    #
    while :; do
        if ! sapi_res=$(curl --max-time 60 --ipv4 -sSf \
          -H 'Accept: application/json' -H 'Content-Type: application/json' \
          "${SAPI_URL}/configs/${ZONE_UUID}"); then
            printf 'WARNING: could not download SAPI config (retrying)\n' >&2
            sleep 2
            continue
        fi

        SAPI_CONFIG=$(json -H <<< "${sapi_res}")

        err=$(json 'code' <<< "${SAPI_CONFIG}")
        if [[ -n "${err}" ]]; then
            printf 'WARNING: error parsing SAPI config (%s) (retrying)\n' >&2
            sleep 2
            continue
        fi
        break
    done
}

function setup_buckets_mdapi {
    local port
    local metrics_port
    local RTPL

    #
    # The default port values here must be kept in sync with the default value of
    # BUCKETS_MDAPI_SERVER_PORT and BUCKETS_MDAPI_METRICS_PORT in
    # sapi_manifests/buckets-mdapi/template and the Makefile.
    #
    port=$(json metadata.BUCKETS_MDAPI_SERVER_PORT <<< "${SAPI_CONFIG}")
    [[ -n "${port}" ]] || port='2030'
    metrics_port=$(json metadata.BUCKETS_MDAPI_METRICS_PORT <<< "${SAPI_CONFIG}")
    [[ -n "${metrics_port}" ]] || port='3020'

    #
    # Regenerate the registrar config with the real port included
    # (the bootstrap one just includes 2030, the default value)
    #
    RTPL=$SVC_ROOT/sapi_manifests/registrar/template
    sed -e "s/@@PORTS@@/${port}/g" ${RTPL}.in > ${RTPL}

    #
    # Wait until config-agent updates registrar's config before restarting
    # registrar.
    #
    svcadm disable -s config-agent
    svcadm enable -s config-agent
    svcadm restart registrar

    svccfg import /opt/smartdc/buckets-mdapi/smf/manifests/buckets-mdapi-setup.xml
    svccfg import /opt/smartdc/buckets-mdapi/smf/manifests/buckets-mdapi.xml

    mdata-put metricPorts $(echo "${metrics_port}")
}


# ---- mainline

source ${DIR}/scripts/util.sh
source ${DIR}/scripts/services.sh

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/buckets-mdapi"

manta_common2_setup "buckets-mdapi"

manta_ensure_zk

echo "setting up buckets-ddapi"
get_sapi_config
setup_buckets_mdapi

manta_common2_setup_log_rotation "buckets-mdapi"

manta_common2_setup_end

exit 0
