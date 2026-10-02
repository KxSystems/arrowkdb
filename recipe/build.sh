#!/bin/bash
set -e
set -x

mkdir -p build
cd build

BUILD_TYPE=Release

# Determine OSX architecture from the target platform
if [[ "$target_platform" == "osx-arm64" ]]; then
    OSX_ARCH="arm64"
elif [[ "$target_platform" == "osx-64" ]]; then
    OSX_ARCH="x86_64"
else
    OSX_ARCH=$(uname -m)
fi

cmake .. \
      -DQMOD=ON \
      -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
      -DARROW_INSTALL=$BUILD_PREFIX \
      -DCMAKE_INSTALL_PREFIX=$PREFIX/lib/q/mod/kx \
      -DCMAKE_OSX_ARCHITECTURES=$OSX_ARCH

cmake --build . --config $BUILD_TYPE

cmake --build . --config $BUILD_TYPE --target install

mkdir -p "${PREFIX}/etc/conda/activate.d" "${PREFIX}/etc/conda/deactivate.d"
cp "${RECIPE_DIR}/activate.sh" "${PREFIX}/etc/conda/activate.d/q-kx-arrow.sh"
cp "${RECIPE_DIR}/deactivate.sh" "${PREFIX}/etc/conda/deactivate.d/q-kx-arrow.sh"
