#!/bin/bash

if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then
  sudo apt update
  sudo apt install -y -V ca-certificates lsb-release wget
  sudo apt install -y -V ca-certificates lsb-release wget
  wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
  sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
  sudo apt update
  sudo apt install -y -V libarrow-dev=9.0.0-1
  sudo apt install -y -V libparquet-dev=9.0.0-1
elif [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
  brew install apache-arrow
  mkdir -p cbuild/install
  cp -r /usr/local/opt/apache-arrow/* cbuild/install
elif [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
  # Create arrow installation directory
  mkdir -p cbuild/install
  export ARROW_INSTALL=$(pwd)/cbuild/install  
  cd cbuild
  # Build and install arrow
  git clone https://github.com/apache/arrow.git
  cd arrow
	git checkout refs/tags/apache-arrow-9.0.0 --
	cd cpp
  mkdir build
  cd build
  cmake -G "Visual Studio 15 2017 Win64" -DARROW_PARQUET=ON -DARROW_WITH_SNAPPY=ON -DARROW_BUILD_STATIC=OFF -DARROW_COMPUTE=OFF -DARROW_DEPENDENCY_USE_SHARED=OFF -DCMAKE_INSTALL_PREFIX=$ARROW_INSTALL ..
  cmake --build . --config Release
  cmake --build . --config Release --target install
  cd ../..
  cd ..
else
  echo "$TRAVIS_OS_NAME is currently not supported"  
fi
