#include "io.h"

#include <fstream>

namespace mybitcask {
namespace io {

class FStreamSequentialWriter : public SequentialWriter {
 public:
  absl::Status Append(absl::Span<const std::uint8_t> src) noexcept override {
    file_.write(reinterpret_cast<const char*>(src.data()), src.size());
    return absl::OkStatus();
  }

  absl::Status Sync() noexcept override {
    file_.flush();
    return absl::OkStatus();
  }

  FStreamSequentialWriter() = delete;
  ~FStreamSequentialWriter() override { file_.close(); }

 private:
  FStreamSequentialWriter(std::ofstream&& file)
      : file_(std::forward<std::ofstream>(file)) {}
  std::ofstream file_;

  friend absl::StatusOr<std::unique_ptr<SequentialWriter>> OpenSequentialWriter(
      ghc::filesystem::path filename);
};

absl::StatusOr<std::unique_ptr<SequentialWriter>> OpenSequentialWriter(
    ghc::filesystem::path filename) {
  std::ofstream file(filename, std::fstream::out);
  if (!file.is_open()) {
    return absl::InternalError(kErrOpenFailed);
  }
  return std::unique_ptr<FStreamSequentialWriter>(
      new FStreamSequentialWriter(std::move(file)));
}

absl::StatusOr<std::size_t> GetFileSize(
    ghc::filesystem::path filename) noexcept {
  std::error_code ec;
  auto size = ghc::filesystem::file_size(filename, ec);
  if (ec) {
    return absl::InternalError(ec.message());
  }
  return size;
}

}  // namespace io
}  // namespace mybitcask
