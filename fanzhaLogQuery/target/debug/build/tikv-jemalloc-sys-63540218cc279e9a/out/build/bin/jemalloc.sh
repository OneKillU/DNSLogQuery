#!/bin/sh

prefix=/Users/warrenwong/Desktop/DNSProject/DNS/RustProject/fanzhaLogQuery/target/debug/build/tikv-jemalloc-sys-63540218cc279e9a/out
exec_prefix=/Users/warrenwong/Desktop/DNSProject/DNS/RustProject/fanzhaLogQuery/target/debug/build/tikv-jemalloc-sys-63540218cc279e9a/out
libdir=${exec_prefix}/lib

DYLD_INSERT_LIBRARIES=${libdir}/libjemalloc.2.dylib
export DYLD_INSERT_LIBRARIES
exec "$@"
