#-------------------------------------------------------------------------
#
# Makefile for pg_shard
#
# Copyright (c) 2014-2015, Citus Data, Inc.
#
#-------------------------------------------------------------------------

EXTENSION = $(shell grep -m 1 '"name":' META.json | sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
EXTVERSION = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

MODULE_big = ${EXTENSION}
OBJS = $(patsubst %.c,%.o,$(wildcard *.c))
TESTS = $(wildcard test/sql/*.sql)
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --outputdir=test --load-language=plpgsql

PG_CPPFLAGS = -std=c99 -Wall -Wextra -Werror -Wno-unused-parameter -Iinclude -I$(libpq_srcdir)

# The launcher regression flag lets us specify a special wrapper to handle
# testing rather than psql directly. Our wrapper swaps in a known worker list.
REGRESS_OPTS += --launcher=./launcher.sh

# pg_shard and CitusDB have several functions that share the same name. When we
# link pg_shard against CitusDB on Linux, the loader resolves to the CitusDB
# function first. We override that behavior and make sure the loader calls the
# pg_shard function instead.
OS := $(shell uname)
ifeq ($(OS), Linux)
	SHLIB_LINK = $(libpq) -Wl,-Bsymbolic
else
	SHLIB_LINK = $(libpq)
endif

DATA = pg_shard--1.2.sql pg_shard--1.0--1.1.sql pg_shard--1.1--1.2.sql
SCRIPTS = bin/copy_to_distributed_table

ifeq ($(enable_coverage),yes)
	PG_CPPFLAGS += --coverage
	SHLIB_LINK  += --coverage
	EXTRA_CLEAN += *.gcno *.gcda test/*.gcno test/*.gcda
endif

# Let the test makefile tell us what objects to build.
include test/Makefile

ifndef NO_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pg_shard
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

# Earlier versions may not define MAJORVERSION
ifndef MAJORVERSION
    MAJORVERSION := $(basename $(VERSION))
endif

PG93 = $(shell echo $(MAJORVERSION) | grep -qE "8\.|9\.[012]" && echo no || echo yes)

# if using a version older than PostgreSQL 9.3, abort
ifeq ($(PG93),no)
    $(error PostgreSQL 9.3 or 9.4 is required to compile this extension)
endif
