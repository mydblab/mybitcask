#include "store.h"
#include "store_filename.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace mybitcask {
namespace store {

absl::StatusOr<size_t> GetFileSize(std::string_view filename) noexcept {
  std::error_code ec;
  auto size = std::filesystem::file_size(filename, ec);
  if (ec) {
    return absl::InternalError(ec.message());
  }
  return size;
}

absl::StatusOr<size_t> Store::ReadAt(uint64_t offset, size_t n,
                                     uint8_t* dst) noexcept {
  return 0;
}

absl::StatusOr<size_t> ReadAt(const Position& pos, size_t n,
                              uint8_t* dst) noexcept {}

absl::Status Store::Append(size_t n, uint8_t* src) noexcept {
  // TODO: lock
  auto file_size =
      std::filesystem::file_size(path_.append(filename_fn_(latest_file_id_)));

  if (file_size >= dead_bytes_threshold_) {
    delete writer_;
    // writer_ = New_log_file();
  }

  return writer_->Append(n, src);
}

absl::Status Store::Sync() noexcept {}

Store::Store(file_id_t latest_file_id, std::filesystem::path path,
             std::function<std::string(file_id_t)> filename_fn,
             size_t dead_bytes_threshold)
    : latest_file_id_(latest_file_id),
      path_(path),
      filename_fn_(filename_fn),
      dead_bytes_threshold_(dead_bytes_threshold),
      writer_(nullptr),
      readers_({}) {}

Store::~Store() {
  // todo
}

io::RandomAccessReader* Store::reader(file_id_t file_id) {
  auto it = readers_.find(file_id);
  if (it == readers_.end()) {
    if (latest_file_id_ < file_id) {
      return nullptr;
    }
    // todo
  }
}

absl::StatusOr<PosixMmapRandomAccessReader*> PosixMmapRandomAccessReader::Open(
    std::string_view filename) {
  int fd = open(filename.data(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return absl::InternalError("open failure.");
  }
  auto file_size_status = GetFileSize(filename);

  if (!file_size_status.ok()) {
    return absl::Status(file_size_status.status());
  }

  void* mmap_base_ =
      mmap(/*addr=*/nullptr, *file_size_status, PROT_READ, MAP_SHARED, fd, 0);

  close(fd);
  return new PosixMmapRandomAccessReader(reinterpret_cast<uint8_t*>(mmap_base_),
                                         *file_size_status);
}

absl::StatusOr<size_t> PosixMmapRandomAccessReader::ReadAt(
    uint64_t offset, size_t n, uint8_t* dst) noexcept {

    }

PosixMmapRandomAccessReader::PosixMmapRandomAccessReader(uint8_t* mmap_base,
                                                         size_t length)
    : mmap_base_(mmap_base), length_(length) {}

PosixMmapRandomAccessReader::~PosixMmapRandomAccessReader() {
  munmap(static_cast<void*>(mmap_base_), length_);
}

absl::StatusOr<size_t> PosixMmapRandomAccessReader::ReadAt(
    uint64_t offset, size_t n, uint8_t* dst) noexcept {}

}  // namespace store
}  // namespace mybitcask
