# Reproducible Ubuntu build

Install build-essential, CMake, pkg-config, libssl-dev, zlib1g-dev and libzip-dev. Use /usr/bin/g++ and /usr/bin/gcc; a compiler wrapper with a private sysroot cannot see system development headers.

```sh
cmake -S . -B build -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DCMAKE_C_COMPILER=/usr/bin/gcc -DUSE_TLS=ON -DUSE_OPEN_SSL=ON -DUSE_ZLIB=ON -DUSE_TEST=ON
cmake --build build -j2
ctest --test-dir build --output-on-failure -R "IXUrlParserTest|IXStrCaseCompareTest|IXExponentialBackoffTest|IXWebSocketPerMessageDeflateCompressorTest"
```

The smoke command covers URL parsing, case comparison, reconnect backoff and compression. Network integration cases require their own local fixtures.
