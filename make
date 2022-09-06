#!/bin/sh

if which gmake 1>&2 2>/dev/null; then
   MAKE=gmake
else
   MAKE=make
fi

$MAKE $*
