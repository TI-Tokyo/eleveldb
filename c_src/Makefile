LEVELDB_VSN ?= "2.0.38rc3"
SNAPPY_VSN ?= "1.1.9"
MSGPACK_TAG ?= "cpp-4.1.1"

BASEDIR = $(shell pwd)

LDFLAGS := $(LDFLAGS) -L$(BASEDIR)/system/lib
LD_LIBRARY_PATH := $(BASEDIR)/system/lib:$(LD_LIBRARY_PATH)
EXTRA_FLAGS = -I $(BASEDIR)/system/include -I. -I $(BASEDIR)/leveldb/include -I $(BASEDIR)/msgpack/include -fPIC
CFLAGS := $(CFLAGS) $(EXTRA_FLAGS)
CXXFLAGS := $(CXXFLAGS) $(EXTRA_FLAGS)

get-deps:
	if [ ! -r snappy-$(SNAPPY_VSN).tar.gz ]; then \
	    wget -q -O snappy-$(SNAPPY_VSN).tar.gz https://github.com/google/snappy/archive/refs/tags/$(SNAPPY_VSN).tar.gz && \
	    tar xzf snappy-$(SNAPPY_VSN).tar.gz; \
	fi && \
	if [ ! -d leveldb ]; then \
	    git clone https://github.com/TI-Tokyo/leveldb && \
	    (cd leveldb && git checkout $(LEVELDB_VSN)) && \
	    (cd leveldb && git submodule update --init); \
	fi && \
	if [ ! -d msgpack ]; then \
	    wget -q -O msgpack-$(MSGPACK_TAG).tar.gz https://github.com/msgpack/msgpack-c/archive/refs/tags/$(MSGPACK_TAG).tar.gz && \
	    tar xzf msgpack-$(MSGPACK_TAG).tar.gz && \
	    mv msgpack-c-$(MSGPACK_TAG) msgpack; \
	fi

compile: get-deps snappy ldb
	cp leveldb/perf_dump leveldb/sst_rewrite leveldb/sst_scan leveldb/leveldb_repair ../priv

ldb:
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb all
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb tools

snappy: system/lib/libsnappy.a

system/lib/libsnappy.a:
	(cd snappy-$(SNAPPY_VSN) && \
	 git submodule update --init && \
	 if [ -r autogen.sh ]; then \
	   ./autogen.sh && ./configure --prefix=$(BASEDIR)/system && make && make install; \
	 else \
	   mkdir build && cd build && \
	   mkdir -p $(BASEDIR)/system && \
	   cmake -D SNAPPY_BUILD_TESTS=0 -D SNAPPY_BUILD_BENCHMARKS=0 \
	         -D CMAKE_INSTALL_PREFIX=$(BASEDIR)/system \
	     ..; \
	  fi && \
	  make && make install)
	mv system/lib64 system/lib || true

clean:
	$(MAKE) -C leveldb clean
	rm -rf system snappy-$(SNAPPY_VSN)/build

test: compile
	$(MAKE) CXXFLAGS="$(CXXFLAGS) -Wno-narrowing" LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb test
