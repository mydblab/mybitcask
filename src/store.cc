#include "store.h"
#include "store_filename.h"

namespace mybitcask {
namespace store {

absl::StatusOr<std::size_t> Store::ReadAt(
    uint64_t offset, absl::Span<std::uint8_t> dst) noexcept {
  auto file_id = offset / dead_bytes_threshold_;
  auto offset_in_file = offset % dead_bytes_threshold_;
  return ReadAt(Position{static_cast<file_id_t>(file_id), offset_in_file}, dst);
}

absl::StatusOr<std::size_t> Store::ReadAt(
    const Position& pos, absl::Span<std::uint8_t> dst) noexcept {
  auto r = reader(pos.file_id);
  if (!r.ok()) {
    return absl::Status(r.status());
  }
  if (*r == nullptr) {
    return absl::OutOfRangeError("Invalid file id");
  }
  return (*r)->ReadAt(pos.offset_in_file, dst);
}

absl::Status Store::Append(absl::Span<const uint8_t> src) noexcept {
  absl::WriterMutexLock l(&latest_file_id_lock_);
  auto file_size = io::GetFileSize(path_ / filename_fn_(latest_file_id_));
  if (!file_size.ok()) {
    return absl::Status(file_size.status());
  }
  if (*file_size >= dead_bytes_threshold_) {
    // The current file has exceeded the threshold. Create a new data file.
    auto writer = io::OpenSequentialFileWriter(filename_fn_(++latest_file_id_));
    if (!writer.ok()) {
      return absl::Status(file_size.status());
    }
    writer_ = *std::move(writer);
  }
  return writer_->Append(src);
}

absl::Status Store::Sync() noexcept { return writer_->Sync(); }

absl::StatusOr<std::uint64_t> Store::Size() const noexcept { return 0; }

Store::Store(file_id_t latest_file_id, ghc::filesystem::path path,
             std::function<std::string(file_id_t)> filename_fn,
             std::size_t dead_bytes_threshold)
    : latest_file_id_(latest_file_id),
      latest_file_id_lock_(),
      path_(path),
      filename_fn_(filename_fn),
      dead_bytes_threshold_(dead_bytes_threshold),
      writer_(nullptr),
      readers_(),
      readers_lock_() {}

absl::StatusOr<io::RandomAccessReader*> Store::reader(file_id_t file_id) {
  readers_lock_.ReaderLock();
  auto it = readers_.find(file_id);
  if (it == readers_.end()) {
    readers_lock_.ReaderUnlock();
    latest_file_id_lock_.ReaderLock();
    if (file_id > latest_file_id_) {
      latest_file_id_lock_.ReaderUnlock();
      return nullptr;
    }
    latest_file_id_lock_.ReaderUnlock();

    readers_lock_.Lock();
    it = readers_.find(file_id);
    if (it == readers_.end()) {
      auto r = io::OpenRandomAccessFileReader(filename_fn_(file_id));
      if (!r.ok()) {
        return absl::Status(r.status());
      }
      it = readers_.insert({file_id, *std::move(r)}).first;
    }
    readers_lock_.Unlock();
  }
  return (*it).second.get();
}
}  // namespace store
}  // namespace mybitcask
