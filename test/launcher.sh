#!/usr/bin/env bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

PGPORT=${PGPORT:-5432}
PSQL=$1
DATADIR=`$PSQL -p$PGPORT -AtXc 'SHOW data_directory' postgres`
PG_WORKER_LIST_CONF=$DATADIR/pg_worker_list.conf

function restore_worker_list {
  if [ -f $PG_WORKER_LIST_CONF.bak ]
  then
    mv -f $PG_WORKER_LIST_CONF.bak $PG_WORKER_LIST_CONF
  fi

  exit
}

trap restore_worker_list HUP INT TERM

# ensure configuration file exists
if [ ! -f $PG_WORKER_LIST_CONF ]
then
  echo > $PG_WORKER_LIST_CONF
fi

if [ -f $PG_WORKER_LIST_CONF.bak ]
then
  >&2 echo 'worker list backup file already present. Please inspect and remove'
  exit 70
fi

sed -i.bak -e's/^/#/g' -e"\$a\\
localhost $PGPORT # added by installcheck\\
adeadhost 5432 # added by installcheck" $PG_WORKER_LIST_CONF

shift
$PSQL -v worker_port=$PGPORT $*

restore_worker_list
