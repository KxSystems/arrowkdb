#!/bin/sh
# q-kx-arrow: prefer this env's own libarrow/libstdc++/etc over the host's.
export Q_KX_ARROW_LD_LIBRARY_PATH_BACKUP="${LD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="${CONDA_PREFIX}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
