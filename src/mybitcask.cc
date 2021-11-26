#include "mybitcask/mybitcask.h"

namespace mybitcask {

MyBitcask::MyBitcask(const ghc::filesystem::path& data_dir,
                     std::uint32_t dead_bytes_threshold, bool checksum)
    : index_(), index_rwlock_() {
  store::LogFiles log_files(data_dir);
  store_ = std::unique_ptr<store::Store>(
      new store::Store(log_files, dead_bytes_threshold));
  log_reader_ = log::LogReader(store_.get(), checksum);
  log_writer_ = log::LogWriter(store_.get());
}

absl::StatusOr<bool> MyBitcask::Get(absl::string_view key, std::string* value,
                                    int try_num) noexcept {
  auto pos = get_position(key);
  if (!pos.has_value()) {
    return false;
  }
  auto found = log_reader_.Read(
      *pos, static_cast<std::uint32_t>(key.size()),
      const_cast<std::uint8_t*>(
          reinterpret_cast<const std::uint8_t*>(value->data())));

  if (!found.ok()) {
    return found.status();
  }
  if (!*found) {
    if (try_num > 0) {
      return Get(key, value, try_num - 1);
    }
    return false;
  }
  return true;
}

absl::Status MyBitcask::Insert(const std::string& key,
                               const std::string& value) noexcept {
  return log_writer_.Append(
      {reinterpret_cast<const std::uint8_t*>(key.data()), key.size()},
      {reinterpret_cast<const std::uint8_t*>(value.data()), value.size()},
      [&](Position pos) {
        absl::WriterMutexLock guard(&index_rwlock_);
        index_.insert({key, pos});
      });
}

absl::Status MyBitcask::Delete(const std::string& key) noexcept {
  return log_writer_.AppendTombstone(
      {reinterpret_cast<const std::uint8_t*>(key.data()), key.size()},
      [&](Position) {
        absl::WriterMutexLock guard(&index_rwlock_);
        index_.erase(key);
      });
}

absl::optional<Position> MyBitcask::get_position(absl::string_view key) {
  absl::ReaderMutexLock guard(&index_rwlock_);
  auto const search = index_.find(key);
  if (search == index_.cend()) {
    return absl::nullopt;
  }
  return search->second;
}
}  // namespace mybitcask
