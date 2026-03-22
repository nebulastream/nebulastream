include ../../config.mk

.PHONY: all test

ifeq ($(WITH_SHARED_LIBRARIES),yes)
LIB_DEP:=../../lib/libmosquitto.so.${SOVERSION}
else
LIB_DEP:=../../lib/libmosquitto.a
endif

all : auth_plugin.so

auth_plugin.so : auth_plugin.c
	$(CC) ${CFLAGS} -fPIC -shared $< -o $@ -I../../lib -I../../src

../lib/libmosquitto.so.${SOVERSION} :
	$(MAKE) -C ../../lib

../lib/libmosquitto.a :
	$(MAKE) -C ../../lib libmosquitto.a

clean :
	-rm -f *.o random_client *.gcda *.gcno

test : all
	./test.py
