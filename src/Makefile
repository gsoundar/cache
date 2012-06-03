# CACHE
# -----
# by Gokul Soundararajan

include config.mk

all: clean buildlib buildexe

buildlib:
	cd include; make
	cd core; make

buildexe: buildlib
	cd main; make

clean:
	cd include; make clean
	cd core; make clean
	cd main; make clean
	rm -rf *.o *.exe *.a *.so *.*~ *~
