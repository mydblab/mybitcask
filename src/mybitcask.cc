#include "mybitcask/mybitcask.h"
#include "store_dbfiles.h"

namespace mybitcask {

absl::Span<const std::uint8_t> MakeU8Span(const std::string& s) {
  return {reinterpret_cast<const std::uint8_t*>(s.data()), s.size()};
}

MyBitcask::MyBitcask(const ghc::filesystem::path& data_dir,
                     std::uint32_t dead_bytes_threshold, bool checksum)
    : index_(), index_rwlock_() {
  store_ = std::unique_ptr<store::Store>(
      new store::Store(data_dir, store::DBFiles(data_dir).latest_file_id(),
                       dead_bytes_threshold));
  log_reader_ = log::Reader(store_.get(), checksum);
  log_writer_ = log::Writer(store_.get());
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
  return log_writer_.Append(MakeU8Span(key), MakeU8Span(value),
                            [&](Position pos) {
                              absl::WriterMutexLock guard(&index_rwlock_);
                              index_.insert({key, pos});
                            });
}

absl::Status MyBitcask::Delete(const std::string& key) noexcept {
  return log_writer_.AppendTombstone(MakeU8Span(key), [&](Position) {
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
