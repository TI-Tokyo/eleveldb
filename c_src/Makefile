.PHONY: get-deps compile ldb snappy test

LEVELDB_VSN ?= 2.0.38
SNAPPY_VSN ?= 1.1.9
MSGPACK_TAG ?= cpp-4.1.1

MAKE ?= make

BASEDIR := $(shell pwd)

LDFLAGS := $(LDFLAGS) -L$(BASEDIR)/system/lib
LD_LIBRARY_PATH := $(BASEDIR)/system/lib:$(LD_LIBRARY_PATH)
EXTRA_FLAGS = -I $(BASEDIR)/system/include -I. -I $(BASEDIR)/leveldb/include -fPIC
CFLAGS := $(CFLAGS) $(EXTRA_FLAGS)
CXXFLAGS := $(CXXFLAGS) $(EXTRA_FLAGS)

get-deps:
	if [ ! -d snappy-$(SNAPPY_VSN) ]; then \
	    wget -q -O snappy-$(SNAPPY_VSN).tar.gz https://github.com/google/snappy/archive/refs/tags/$(SNAPPY_VSN).tar.gz && \
	    tar xzf snappy-$(SNAPPY_VSN).tar.gz; \
	fi && \
	if [ ! -d leveldb ]; then \
	    git clone --depth 1 --shallow-submodules -b $(LEVELDB_VSN) \
	      https://github.com/TI-Tokyo/leveldb && \
	    (cd leveldb && git submodule update --init) \
	fi && \
	if [ ! -d msgpack ]; then \
	    wget -q -O msgpack-$(MSGPACK_TAG).tar.gz https://github.com/msgpack/msgpack-c/archive/refs/tags/$(MSGPACK_TAG).tar.gz && \
	    tar xzf msgpack-$(MSGPACK_TAG).tar.gz && \
	    mv msgpack-c-$(MSGPACK_TAG) msgpack; \
	fi
	mkdir -p system/lib

compile: get-deps ldb
	cp leveldb/perf_dump leveldb/sst_rewrite leveldb/sst_scan leveldb/leveldb_repair ../priv

ldb: snappy
	echo "BASEDIR: $(BASEDIR)"
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" \
	    -C leveldb all tools && \
	  mv leveldb/libleveldb.a system/lib \

snappy:
	(cd snappy-$(SNAPPY_VSN) && \
	 if [ -r autogen.sh ]; then \
	   ./autogen.sh && ./configure --prefix=$(BASEDIR)/system && $(MAKE) && $(MAKE) install; \
	 else \
	   rm -rf build && mkdir build && cd build && \
	   cmake -D SNAPPY_BUILD_TESTS=0 -D SNAPPY_BUILD_BENCHMARKS=0 \
	         -D CMAKE_INSTALL_PREFIX=$(BASEDIR)/system \
	     ..; \
	  fi && \
	  $(MAKE) && $(MAKE) install)
	(cd system && if [ -d lib64 ]; then cp -a lib64/* lib; fi)

clean:
	$(MAKE) -C leveldb clean
	rm -rf system snappy-$(SNAPPY_VSN)/build

test: compile
	$(MAKE) CXXFLAGS="$(CXXFLAGS) -Wno-narrowing" LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb test
