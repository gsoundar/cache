# CACHE
# -----
# by Gokul Soundararajan

include ../config.mk

all: build
clean:
	rm -rf *.o *.exe *.a *.so *.*~ *~

build: hash_cache.exe

hash_cache.exe: hash_cache.c
	${CC} ${CFLAGS} ${INCLUDES} hash_cache.c -o hash_cache.exe ${LIBDIRS} ${LIBS}





