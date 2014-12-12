#!/bin/sh

PGPORT=${PGPORT:-5432}
BINDIR=`pg_config --bindir`
PSQL=$BINDIR/psql
DATADIR=`$PSQL -p$PGPORT -AtXc 'SHOW data_directory' postgres`
PG_WORKER_LIST_CONF=$DATADIR/pg_worker_list.conf

function restore_worker_list {
  if [ -e $PG_WORKER_LIST_CONF.bak ]
  then
    mv -f $PG_WORKER_LIST_CONF.bak $PG_WORKER_LIST_CONF
  fi

  exit
}

trap restore_worker_list HUP INT TERM

if [ -e $PG_WORKER_LIST_CONF.bak ]
then
  >&2 echo 'worker list backup file already present. Please inspect and remove'
  exit 70
fi

sed -E -i '.bak' -e 's/^/#/g' -e "\$a\\
localhost $PGPORT # added by installcheck\\
adeadhost 5432 # added by installcheck" $PG_WORKER_LIST_CONF

$*

restore_worker_list
