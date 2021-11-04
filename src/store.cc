#include "store.h"
#include "store_filename.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <cstring>

namespace mybitcask {
namespace store {

absl::StatusOr<std::size_t> GetFileSize(std::string_view filename) noexcept {
  std::error_code ec;
  auto size = std::filesystem::file_size(filename, ec);
  if (ec) {
    return absl::InternalError(ec.message());
  }
  return size;
}

absl::StatusOr<std::size_t> Store::ReadAt(
    uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept {
  return 0;
}

absl::StatusOr<std::size_t> Store::ReadAt(
    const Position& pos, absl::Span<std::uint8_t> dst) const noexcept {}

absl::Status Store::Append(absl::Span<uint8_t> src) noexcept {
  // TODO: lock
  auto file_size =
      std::filesystem::file_size(path_.append(filename_fn_(latest_file_id_)));

  if (file_size >= dead_bytes_threshold_) {
    delete writer_;
    // writer_ = New_log_file();
  }
  return writer_->Append(src);
}

absl::Status Store::Sync() noexcept {}

Store::Store(file_id_t latest_file_id, std::filesystem::path path,
             std::function<std::string(file_id_t)> filename_fn,
             std::size_t dead_bytes_threshold)
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

absl::StatusOr<std::unique_ptr<MmapRandomAccessReader>>
MmapRandomAccessReader::Open(std::string_view filename) {
  int fd = open(filename.data(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return absl::InternalError(kErrOpenFailed);
  }
  auto status_file_size = GetFileSize(filename);

  if (!status_file_size.ok()) {
    return absl::Status(status_file_size.status());
  }

  void* mmap_base_ =
      mmap(/*addr=*/nullptr, *status_file_size, PROT_READ, MAP_SHARED, fd, 0);

  close(fd);
  return std::unique_ptr<MmapRandomAccessReader>(new MmapRandomAccessReader(
      reinterpret_cast<std::uint8_t*>(mmap_base_), *status_file_size));
}

absl::StatusOr<std::size_t> MmapRandomAccessReader::ReadAt(
    uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept {
  if (offset >= length_ - 1) {
    return absl::OutOfRangeError("offset is too large");
  }
  auto actual_size =
      std::min(static_cast<std::size_t>(length_ - offset), dst.size());
  std::memcpy(dst.data(), mmap_base_ + offset, actual_size);
  return actual_size;
}

MmapRandomAccessReader::MmapRandomAccessReader(std::uint8_t* mmap_base,
                                               std::size_t length)
    : mmap_base_(mmap_base), length_(length) {}

MmapRandomAccessReader::~MmapRandomAccessReader() {
  munmap(static_cast<void*>(mmap_base_), length_);
}

absl::StatusOr<std::unique_ptr<FStreamSequentialWriter>>
FStreamSequentialWriter::Open(std::string_view filename) {
  std::ofstream file(filename, std::fstream::out);
  if (!file.is_open()) {
    return absl::InternalError(kErrOpenFailed);
  }
  return std::unique_ptr<FStreamSequentialWriter>(
      new FStreamSequentialWriter(std::move(file)));
}

absl::Status FStreamSequentialWriter::Append(
    absl::Span<std::uint8_t> src) noexcept {
  file_.write(reinterpret_cast<char*>(src.data()), src.size());
}

absl::Status FStreamSequentialWriter::Sync() noexcept { file_.flush(); }

FStreamSequentialWriter::~FStreamSequentialWriter() { file_.close(); }

FStreamSequentialWriter::FStreamSequentialWriter(std::ofstream&& file)
    : file_(std::forward<std::ofstream>(file)) {}

}  // namespace store
}  // namespace mybitcask
