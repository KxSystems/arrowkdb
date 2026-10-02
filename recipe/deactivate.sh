#!/bin/sh
# q-kx-arrow: restore LD_LIBRARY_PATH as it was before activation.
if [ -n "${Q_KX_ARROW_LD_LIBRARY_PATH_BACKUP:-}" ]; then
  export LD_LIBRARY_PATH="${Q_KX_ARROW_LD_LIBRARY_PATH_BACKUP}"
else
  unset LD_LIBRARY_PATH
fi
unset Q_KX_ARROW_LD_LIBRARY_PATH_BACKUP
