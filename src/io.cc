#include "mybitcask/internal/io.h"

#include <fstream>

namespace mybitcask {
namespace io {

class FStreamSequentialWriter : public SequentialWriter {
 public:
  FStreamSequentialWriter() = delete;
  ~FStreamSequentialWriter() override { file_.close(); }

  absl::StatusOr<std::uint32_t> Append(
      absl::Span<const std::uint8_t> src) noexcept override {
    file_.write(reinterpret_cast<const char*>(src.data()), src.size());
    if (file_.fail()) {
      return absl::InternalError("write failed");
    }
    std::uint32_t offset = current_offset_;
    current_offset_ += src.size();
    return offset;
  }

  absl::Status Sync() noexcept override {
    file_.flush();
    return absl::OkStatus();
  }

  std::uint32_t Size() const noexcept override { return current_offset_; }

 private:
  FStreamSequentialWriter(ghc::filesystem::path&& filename,
                          std::ofstream&& file, std::uint32_t current_offset)
      : filename_(std::move(filename)),
        file_(std::move(file)),
        current_offset_(current_offset) {}

  std::ofstream file_;
  std::uint32_t current_offset_;
  const ghc::filesystem::path filename_;

  friend absl::StatusOr<std::unique_ptr<SequentialWriter>>
  OpenSequentialFileWriter(ghc::filesystem::path&& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<SequentialWriter>> OpenSequentialFileWriter(
    ghc::filesystem::path&& filename) noexcept {
  std::ofstream file(
      filename, std::fstream::out | std::fstream::app | std::fstream::binary);
  if (!file.is_open()) {
    return absl::InternalError(kErrOpenFailed);
  }
  auto filesize = GetFileSize(filename);
  if (!filesize.ok()) {
    return absl::Status(filesize.status());
  }
  return std::unique_ptr<FStreamSequentialWriter>(new FStreamSequentialWriter(
      std::move(filename), std::move(file), *filesize));
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
