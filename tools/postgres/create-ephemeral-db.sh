#!/usr/bin/env bash

set -e

export PG_TMP=$(pg_tmp -w 5 -t -o "--client-min-messages=warning \
    --log-min-duration-statement=500 --log-connections=off --log-disconnections=off")
export BASEDIR=$(cd `dirname "$0"` && pwd)

setup_database() {
    psql $PG_TMP -c 'CREATE ROLE postgres SUPERUSER LOGIN;'
    psql -U postgres $PG_TMP -c 'ALTER DATABASE test SET timezone TO "UTC";'

}

main() {

    output=`setup_database` \
        || echo $output 1>&2
}

main
echo $PG_TMP
