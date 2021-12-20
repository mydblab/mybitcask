#include "mybitcask/internal/io.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <cstring>

namespace mybitcask {
namespace io {

class PosixMmapRandomAccessFileReader final : public RandomAccessReader {
 public:
  PosixMmapRandomAccessFileReader() = delete;
  ~PosixMmapRandomAccessFileReader() override {
    ::munmap(static_cast<void*>(mmap_base_), length_);
  }

  absl::StatusOr<std::size_t> ReadAt(
      std::uint64_t offset, absl::Span<std::uint8_t> dst) noexcept override {
    std::size_t offset_size = static_cast<std::size_t>(offset);
    if (offset_size + dst.size() > length_) {
      return 0;
    }
    auto actual_size = std::min(length_ - offset_size, dst.size());
    std::memcpy(dst.data(), mmap_base_ + offset_size, actual_size);
    return actual_size;
  }

 private:
  PosixMmapRandomAccessFileReader(std::uint8_t* mmap_base, std::size_t length)
      : mmap_base_(mmap_base), length_(length) {}
  std::uint8_t* const mmap_base_;
  const std::size_t length_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenMmapRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept;
};

class PosixRandomAccessFileReader final : public RandomAccessReader {
 public:
  PosixRandomAccessFileReader() = delete;
  ~PosixRandomAccessFileReader() override { ::close(fd_); }

  absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) noexcept override {
    ssize_t actual_size =
        ::pread(fd_, dst.data(), dst.size(), static_cast<off_t>(offset));
    if (actual_size < 0) {
      return absl::InternalError("pread failed");
    }
    return actual_size;
  }

 private:
  PosixRandomAccessFileReader(const int fd) : fd_(fd) {}
  const int fd_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>>
OpenMmapRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept {
  int fd = ::open(filename.string().c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return absl::InternalError(kErrOpenFailed);
  }
  auto file_size = GetFileSize(filename);

  if (!file_size.ok()) {
    return absl::Status(file_size.status());
  }

  void* mmap_base =
      ::mmap(/*addr=*/nullptr, *file_size, PROT_READ, MAP_SHARED, fd, 0);

  ::close(fd);
  return std::unique_ptr<RandomAccessReader>(
      new PosixMmapRandomAccessFileReader(
          reinterpret_cast<std::uint8_t*>(mmap_base), *file_size));
}

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessFileReader(
    ghc::filesystem::path&& filename) noexcept {
  int fd = ::open(filename.string().c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return absl::InternalError(kErrOpenFailed);
  }
  return std::unique_ptr<RandomAccessReader>(
      new PosixRandomAccessFileReader(fd));
}

}  // namespace io
}  // namespace mybitcask
