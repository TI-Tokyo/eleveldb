#!/bin/sh

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

LEVELDB_VSN="2.0.36"
MSGPACK_TAG="cpp-4.1.1"
SNAPPY_VSN="1.0.4"

set -e

if [ `basename $PWD` != "c_src" ]; then
    # originally "pushd c_src" of bash
    # but no need to use directory stack push here
    cd c_src
fi

BASEDIR="$PWD"

# detecting gmake and if exists use it
# if not use make
# (code from github.com/tuncer/re2/c_src/build_deps.sh
which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

# Changed "make" to $MAKE

get_dep_leveldb () {
    if [ ! -d leveldb ]; then
        git clone --depth=1 --shallow-submodules --branch=$LEVELDB_VSN https://github.com/basho/leveldb
        (cd leveldb && git submodule update --init)
    else
        (cd leveldb && dl=$(git diff $LEVELDB_VSN |wc -l) && [ $dl != 0 ] && >&2 echo "\033[0;31m WARN - local leveldb is out of sync with remote $LEVELDB_VSN\033[0m") || :
    fi
}

get_dep_msgpack () {
    if [ ! -d msgpack ]; then
        wget -q https://github.com/msgpack/msgpack-c/archive/refs/tags/$MSGPACK_TAG.tar.gz
        tar xzf $MSGPACK_TAG.tar.gz && rm $MSGPACK_TAG.tar.gz
        mv msgpack-c-$MSGPACK_TAG msgpack
    fi
}

case "$1" in
    rm-deps)
        rm -rf leveldb msgpack system snappy-$SNAPPY_VSN
        ;;

    clean)
        rm -rf system snappy-$SNAPPY_VSN msgpack
        if [ -d leveldb ]; then
            (cd leveldb && $MAKE clean)
        fi
        rm -f ../priv/leveldb_repair ../priv/sst_scan ../priv/sst_rewrite ../priv/perf_dump
        ;;

    test)
        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include $BASEDIR/msgpack/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"
        export LEVELDB_VSN="$LEVELDB_VSN"

        (cd leveldb && $MAKE check)

        ;;

    get-deps)
        get_dep_leveldb
        get_dep_msgpack
        ;;

    *)
        TARGET_OS=`uname -s`

        get_dep_msgpack

        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        # On GCC, we pick libc's memcmp over GCC's memcmp via -fno-builtin-memcmp
        if [ "$TARGET_OS" = "Darwin" ]; then
            export MACOSX_DEPLOYMENT_TARGET=10.8
            export CFLAGS="$CFLAGS -stdlib=libc++"
            export CXXFLAGS="$CXXFLAGS -stdlib=libc++"
        fi

        if [ ! -d snappy-$SNAPPY_VSN ]; then
            tar -xzf snappy-$SNAPPY_VSN.tar.gz
            (cd snappy-$SNAPPY_VSN && ./configure --disable-shared --prefix=$BASEDIR/system --libdir=$BASEDIR/system/lib --with-pic)
        fi

        if [ ! -f system/lib/libsnappy.a ]; then
            (cd snappy-$SNAPPY_VSN && $MAKE && $MAKE install)
        fi


        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"
        export LEVELDB_VSN="$LEVELDB_VSN"

        get_dep_leveldb

        (cd leveldb && $MAKE all)
        (cd leveldb && $MAKE tools)
        (cp leveldb/perf_dump leveldb/sst_rewrite leveldb/sst_scan leveldb/leveldb_repair ../priv)

        ;;
esac
