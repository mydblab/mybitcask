#include "mybitcask/internal/io.h"

#include <fstream>

namespace mybitcask {
namespace io {

class FStreamSequentialWriter : public SequentialWriter {
 public:
  FStreamSequentialWriter() = delete;
  ~FStreamSequentialWriter() override { file_.close(); }

  absl::Status Append(
      absl::Span<const std::uint8_t> src,
      std::function<void()> success_callback) noexcept override {
    file_.write(reinterpret_cast<const char*>(src.data()), src.size());
    return absl::OkStatus();
  }

  absl::Status Sync() noexcept override {
    file_.flush();
    return absl::OkStatus();
  }

  absl::StatusOr<std::uint64_t> Size() const noexcept override {
    return GetFileSize(filename_);
  }

 private:
  FStreamSequentialWriter(ghc::filesystem::path&& filename,
                          std::ofstream&& file)
      : filename_(std::move(filename)), file_(std::move(file)) {}

  std::ofstream file_;
  const ghc::filesystem::path filename_;

  friend absl::StatusOr<std::unique_ptr<SequentialWriter>>
  OpenSequentialFileWriter(ghc::filesystem::path&& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<SequentialWriter>> OpenSequentialFileWriter(
    ghc::filesystem::path&& filename) noexcept {
  std::ofstream file(filename, std::fstream::out | std::fstream::app);
  if (!file.is_open()) {
    return absl::InternalError(kErrOpenFailed);
  }
  return std::unique_ptr<FStreamSequentialWriter>(new FStreamSequentialWriter(
      std::forward<ghc::filesystem::path>(filename), std::move(file)));
}

absl::StatusOr<std::size_t> GetFileSize(
    const ghc::filesystem::path& filename) noexcept {
  std::error_code ec;
  auto size = ghc::filesystem::file_size(filename, ec);
  if (ec) {
    return absl::InternalError(ec.message());
  }
  return size;
}

}  // namespace io
}  // namespace mybitcask
