# CACHE
# -----
# by Gokul Soundararajan

include ../config.mk

all: build
clean:
	rm -rf *.o *.exe *.a *.so *.*~ *~

build: hashtable.o bpool.o
	ar rcs libcachecore.a *.o

hashtable.o: hashtable.c
	${CC} ${CFLAGS} ${INCLUDES} -c hashtable.c -o hashtable.o
bpool.o: bpool.c
	${CC} ${CFLAGS} ${INCLUDES} -c bpool.c -o bpool.o


