#-------------------------------------------------------------------------
#
# Makefile for pg_shard
#
# Copyright (c) 2014, Citus Data, Inc.
#
#-------------------------------------------------------------------------

MODULE_big = pg_shard
OBJS = connection.o create_shards.o citus_metadata_sync.o distribution_metadata.o \
	   extend_ddl_commands.o generate_ddl_commands.o pg_shard.o prune_shard_list.o \
	   repair_shards.o ruleutils.o

PG_CPPFLAGS = -std=c99 -Wall -Wextra -I$(libpq_srcdir)

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

EXTENSION = pg_shard
DATA = pg_shard--1.1.sql pg_shard--1.0--1.1.sql

# Default to 5432 if PGPORT is undefined. Replace placeholders in our tests
# with actual port number in order to anticipate correct output during tests.
PGPORT ?= 5432
sql/%: sql/%.tmpl
	sed -e 's/$$PGPORT/${PGPORT}/g' $^ > $@

expected/%: expected/%.tmpl
	sed -e 's/$$PGPORT/${PGPORT}/g' $^ > $@

# REGRESS_PREP is a make target executed by the PGXS build system before any
# tests are run. We use it to trigger variable interpolation in our tests.
REGRESS_PREP = sql/connection.sql expected/connection.out sql/create_shards.sql \
			   expected/create_shards.out sql/repair_shards.sql \
			   expected/repair_shards.out  expected/modifications.out
REGRESS = init connection distribution_metadata extend_ddl_commands \
		  generate_ddl_commands create_shards prune_shard_list repair_shards \
		  modifications queries utilities citus_metadata_sync

# The launcher regression flag lets us specify a special wrapper to handle
# testing rather than psql directly. Our wrapper swaps in a known worker list.
REGRESS_OPTS = --launcher=./launcher.sh

EXTRA_CLEAN += ${REGRESS_PREP}

ifeq ($(enable_coverage),yes)
	PG_CPPFLAGS += --coverage
	SHLIB_LINK  += --coverage
	EXTRA_CLEAN += *.gcno *.gcda test/*.gcno test/*.gcda
endif

# Let the test's makefile tell us what objects to build.
include test/Makefile

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Build the 9.3- or 9.4-derived ruleutils, depending upon active version
ifneq (,$(findstring $(MAJORVERSION), 9.3))
	RULEUTILS_IMPL := ruleutils_93.c
else ifneq (,$(findstring $(MAJORVERSION), 9.4))
	RULEUTILS_IMPL := ruleutils_94.c
else
	# Error out if too old altogether
	$(error PostgreSQL 9.3 or 9.4 is required to compile this extension)
endif

# Same as implicit %.o rule, except building ruleutils.o from -93/94
ruleutils.o: $(RULEUTILS_IMPL)
	$(COMPILE.c) $(OUTPUT_OPTION) $<
