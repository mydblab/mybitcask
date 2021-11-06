#include "io.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>

namespace mybitcask {
namespace io {

class PosixMmapRandomAccessReader : public RandomAccessReader {
 public:
  PosixMmapRandomAccessReader() = delete;
  ~PosixMmapRandomAccessReader() override {
    munmap(static_cast<void*>(mmap_base_), length_);
  }

  absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept override {
    if (offset >= length_ - 1) {
      return absl::OutOfRangeError("offset is too large");
    }
    auto actual_size =
        std::min(static_cast<std::size_t>(length_ - offset), dst.size());
    std::memcpy(dst.data(), mmap_base_ + offset, actual_size);
    return actual_size;
  }

 private:
  PosixMmapRandomAccessReader(std::uint8_t* mmap_base, std::size_t length)
      : mmap_base_(mmap_base), length_(length) {}
  std::uint8_t* const mmap_base_;
  const std::size_t length_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessReader(const ghc::filesystem::path& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessReader(
    const ghc::filesystem::path& filename) noexcept {
  int fd = ::open(filename.generic_string().data(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return absl::InternalError(kErrOpenFailed);
  }
  auto status_file_size = GetFileSize(filename);

  if (!status_file_size.ok()) {
    return absl::Status(status_file_size.status());
  }

  void* mmap_base_ =
      ::mmap(/*addr=*/nullptr, *status_file_size, PROT_READ, MAP_SHARED, fd, 0);

  ::close(fd);
  return std::unique_ptr<RandomAccessReader>(new PosixMmapRandomAccessReader(
      reinterpret_cast<std::uint8_t*>(mmap_base_), *status_file_size));
}

}  // namespace io
}  // namespace mybitcask
