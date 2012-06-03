# Build Configuration
CC=gcc
CCC=g++
CFLAGS=-g -Wall -O4 -finline-functions -m32
CODE_BASE=.
CACHE_INCLUDES=-I${CODE_BASE}/src/include
CACHE_LIBDIRS=-L${CODE_BASE}/src/core
CACHE_LIBS=-lcachecore

INCLUDES=${CACHE_INCLUDES}
LIBDIRS=${CACHE_LIBDIRS}
LIBS=${CACHE_LIBS}



