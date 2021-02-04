#!/bin/bash

if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
  sudo apt update
  sudo apt install -y -V ca-certificates lsb-release wget
  if [ $(lsb_release --codename --short) = "stretch" ]; then
    sudo tee /etc/apt/sources.list.d/backports.list <<APT_LINE
deb http://deb.debian.org/debian $(lsb_release --codename --short)-backports main
APT_LINE
  fi
  wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb
  sudo apt install -y -V ./apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb
  sudo apt update
  sudo apt install -y -V libarrow-dev
  sudo apt install -y -V libparquet-dev
elif [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
  brew install apache-arrow
  mkdir -p cbuild/install
  cp -r /usr/local/opt/apache-arrow/* cbuild/install
elif [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
  # Create arrow installation directory
  mkdir -p cbuild/install
  export ARROW_INSTALL=$(pwd)/cbuild/install  
  cd cbuild
  # Build and install snappy
  git clone https://github.com/google/snappy.git
  cd snappy
  mkdir install
  export SNAPPY_INSTALL=$(pwd)/install
  mkdir build
  cd build
  cmake -G "Visual Studio 15 2017 Win64" -DCMAKE_INSTALL_PREFIX=$SNAPPY_INSTALL -DSNAPPY_BUILD_BENCHMARKS:BOOL=0 -DSNAPPY_BUILD_TESTS:BOOL=0 ..
  cmake --build . --config Release
  cmake --build . --config Release --target install
  cd ../..
  # Build and install arrow
  git clone https://github.com/apache/arrow.git --branch apache-arrow-2.0.0
  cd arrow/cpp
  mkdir build
  cd build
  cmake -G "Visual Studio 15 2017 Win64" -DARROW_PARQUET=ON -DARROW_WITH_SNAPPY=ON -DARROW_BUILD_STATIC=OFF -DSnappy_LIB=$SNAPPY_INSTALL/lib/snappy.lib -DSnappy_INCLUDE_DIR=$SNAPPY_INSTALL/include -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL ..
  cmake --build . --config Release
  cmake --build . --config Release --target install
  cd ../..
  cd ..
else
  echo "$TRAVIS_OS_NAME is currently not supported"  
fi
