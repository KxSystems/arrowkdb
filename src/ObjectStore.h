#ifndef __OBJECT_STORE_H__
#define __OBJECT_STORE_H__

#include <memory>

#include <arrow/util/config.h>
#include <arrow/filesystem/s3fs.h>


#include "ArrowKdb.h"

namespace kx {
namespace arrowkdb {

    std::shared_ptr<arrow::fs::S3FileSystem> getS3FileSystem();

} // namespace arrowkdb
} // namespace kx

#ifndef DISABLE_GCS
#include <arrow/filesystem/gcsfs.h>

namespace kx {
namespace arrowkdb {
    std::shared_ptr<arrow::fs::GcsFileSystem> getGCSFileSystem();
} // namespace arrowkdb
} // namespace kx

#endif

#ifndef DISABLE_AZURE
#if ARROW_VERSION_MAJOR >= 16
#include <arrow/filesystem/azurefs.h>

namespace kx {
namespace arrowkdb {
    std::shared_ptr<arrow::fs::AzureFileSystem> getAzureFileSystem(const arrow::fs::AzureOptions& options);

} // namespace arrowkdb
} // namespace kx

#endif
#endif

#endif
