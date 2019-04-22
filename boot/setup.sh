#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2019, Joyent, Inc.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

role=boray
SOURCE="${BASH_SOURCE[0]}"
if [[ -h $SOURCE ]]; then
    SOURCE="$(readlink "$SOURCE")"
fi
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROFILE=/root/.bashrc
PG_USER=boray
SVC_ROOT=/opt/smartdc/boray
ZONE_UUID=`/usr/bin/zonename`

export PATH=$SVC_ROOT/bin:$SVC_ROOT/build/node/bin:/opt/local/bin:/usr/sbin/:/usr/bin:$PATH

function manta_setup_boray {
    svccfg import /opt/smartdc/muppet/smf/manifests/boray.xml
    svcadm enable boray
    [[ $? -eq 0 ]] || fatal "Unable to start boray"
}

echo "Running common setup scripts"
manta_common_presetup

echo "Adding local manifest directories"
manta_add_manifest_dir "/opt/smartdc/boray"

manta_common_setup "boray" 0

manta_ensure_zk

echo "Setting up Boray"
manta_setup_boray_config

manta_common_setup_end

exit 0
