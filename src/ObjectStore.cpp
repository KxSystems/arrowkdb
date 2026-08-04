#include <arrow/result.h>
#include "ObjectStore.h"

namespace kx {
namespace arrowkdb {

    std::shared_ptr<arrow::fs::S3FileSystem> getS3FileSystem() {
        static std::shared_ptr<arrow::fs::S3FileSystem> instance = [](){
            arrow::fs::InitializeS3(arrow::fs::S3GlobalOptions{arrow::fs::S3LogLevel::Error});
            arrow::fs::S3Options options = arrow::fs::S3Options::Defaults();
            auto maybe_fs = arrow::fs::S3FileSystem::Make(options);
            if (!maybe_fs.ok()) {
                throw std::runtime_error("S3 filesystem could not be initialized");
            }
            return *maybe_fs;
        }();
        return instance;
    }

#ifndef DISABLE_GCS
    std::shared_ptr<arrow::fs::GcsFileSystem> getGCSFileSystem() {
        static std::shared_ptr<arrow::fs::GcsFileSystem> instance = [](){
            arrow::fs::GcsOptions options = arrow::fs::GcsOptions::Defaults();
#if ARROW_VERSION_MAJOR < 19
             return arrow::fs::GcsFileSystem::Make(options);
#else
            auto maybe_fs = arrow::fs::GcsFileSystem::Make(options);
            if (!maybe_fs.ok()) {
                throw std::runtime_error("GCS filesystem could not be initialized");
            }
            return *maybe_fs;
#endif
        }();
        return instance;
    }
#endif

#ifndef DISABLE_AZURE
#if ARROW_VERSION_MAJOR >= 16
    std::shared_ptr<arrow::fs::AzureFileSystem> getAzureFileSystem(const arrow::fs::AzureOptions&  options) {
        static std::shared_ptr<arrow::fs::AzureFileSystem> instance = [&options](){
            auto maybe_fs = arrow::fs::AzureFileSystem::Make(options);
            if (!maybe_fs.ok()) {
                throw std::runtime_error("Azure filesystem could not be initialized");
            }
            return *maybe_fs;
        }();
        return instance;
    }
#endif
#endif

} // namespace arrowkdb
} // namespace kx
