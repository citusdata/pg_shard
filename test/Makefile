# add objects referenced by the test function headers
PG_CPPFLAGS += -Itest/include
OBJS += $(patsubst %.c,%.o,$(wildcard test/src/*.c))
EXTRA_CLEAN += $(addprefix test/src/,*.gcno *.gcda)
