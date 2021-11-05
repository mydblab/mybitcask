#include "store.h"
#include "store_filename.h"

namespace mybitcask {
namespace store {

absl::StatusOr<std::size_t> Store::ReadAt(
    uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept {
  return 0;
}

absl::StatusOr<std::size_t> Store::ReadAt(
    const Position& pos, absl::Span<std::uint8_t> dst) const noexcept {}

absl::Status Store::Append(absl::Span<const uint8_t> src) noexcept {
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

const io::RandomAccessReader* Store::reader(file_id_t file_id) {
  auto it = readers_.find(file_id);
  if (it == readers_.end()) {
    if (latest_file_id_ < file_id) {
      return nullptr;
    }
    // todo
  }
}
}  // namespace store
}  // namespace mybitcask
