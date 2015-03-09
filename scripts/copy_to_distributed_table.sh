#!/bin/sh

format='text'
options='OIDS false, FREEZE false'
schema='public'

while getopts ':BCc:d:e:Hh:n:q:T' o; do
	case "${o}" in
		B)
			format='binary'
			;;
		C)
			format='csv'
			;;
		c)
			encoding=`echo ${OPTARG} | sed s/\'/\'\'/g`
			options="${options}, ENCODING '${encoding}'"
			;;
		d)
			delimiter=`echo ${OPTARG} | sed s/\'/\'\'/g`
			options="${options}, DELIMITER '${delimiter}'"
			;;
		e)
			escape=`echo ${OPTARG} | sed s/\'/\'\'/g`
			options="${options}, ESCAPE '${escape}'"
			;;
		H)
			options="${options}, HEADER true"
			;;
		h)
			usage
			;;
		n)
			null=`echo ${OPTARG} | sed s/\'/\'\'/g`
			options="${options}, NULL '${null}'"
			;;
		q)
			quote=`echo ${OPTARG} | sed s/\'/\'\'/g`
			options="${options}, QUOTE '${quote}'"
			;;
		s)
			schema=`echo ${OPTARG} | sed s/\'/\'\'/g`
			;;
		T)
			format='text'
			;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

options="${options}, FORMAT ${format}"
filename=$1
tablename=$2

if [ -z "${filename}" ] || [ -z "${tablename}" ]; then
	usage
fi

filename=`echo ${filename} | sed s/\'/\'\'/g`
facadename=`echo "${tablename}_copy_facade" | sed s/\"/\"\"/g`
tablename=`echo ${tablename} | sed s/\'/\'\'/g`

psql -X << E_O_SQL

\set QUIET on
\set ON_ERROR_ROLLBACK off
\pset format unaligned
\pset tuples_only on
\set ON_ERROR_STOP on

\o /dev/null

/*
 * Use a session-bound counter to keep track of the number of rows inserted: we
 * can't roll back so we need to tell the user how many rows were inserted. Due
 * to the trigger implementation, the COPY will report zero rows, so we count
 * them manually for a better experience.
 */
CREATE TEMPORARY SEQUENCE rows_inserted CACHE 100000;

SELECT prepare_distributed_table_for_copy('${schema}.${tablename}', 'rows_inserted');

/* Don't stop if copy errors out: continue to print file name and row count*/
\set ON_ERROR_STOP off

\copy pg_temp.${facadename} from ${filename} with ($options)

\o

SELECT currval('rows_inserted');

E_O_SQL
