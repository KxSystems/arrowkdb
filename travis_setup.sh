#!/bin/bash

mkdir cbuild

# Download the protobuf c++ source code
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.12.3/protobuf-cpp-3.12.3.tar.gz
tar xvf protobuf-cpp-3.12.3.tar.gz -C ./cbuild --strip-components=1

if [[ "$TRAVIS_OS_NAME" == "osx" || "$TRAVIS_OS_NAME" == "linux" ]]; then
  # Build and install protobuf to /usr
  cd cbuild
  ./configure --prefix=/usr "CFLAGS=-fPIC" "CXXFLAGS=-fPIC"
  make
  sudo make install
  cd ..
elif [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
  # Build and install protobuf to cbuild/install
  mkdir cbuild/install
  mkdir cbuild/cmake/solution
  cd cbuild/cmake/solution
  cmake -G "Visual Studio 15 2017 Win64" -DCMAKE_INSTALL_PREFIX=../../install ..
  cmake --build . --config Release
  cmake --build . --config Release --target install
  cd ../../..
else
  echo "$TRAVIS_OS_NAME is currently not supported"  
fi
