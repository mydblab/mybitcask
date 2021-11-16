#include "mybitcask/mybitcask.h"

namespace mybitcask {

MyBitcask::MyBitcask() : index_(), index_rwlock_() {}

absl::StatusOr<bool> MyBitcask::Get(absl::string_view key, std::string* value,
                                    int try_num) noexcept {
  auto pos = get_position(key);
  if (!pos.has_value()) {
    return false;
  }
  auto entry_opt = log_reader_->Read(*pos);
  if (!entry_opt.ok()) {
    return entry_opt.status();
  }
  if (!entry_opt->has_value()) {
    if (try_num > 0) {
      return Get(key, value, try_num - 1);
    }
    return false;
  }
  auto val = (*entry_opt)->value();
  value->assign(reinterpret_cast<const char*>(val.data()), val.size());
  return true;
}

absl::Status MyBitcask::Insert(const std::string& key,
                               const std::string& value) noexcept {
  return log_writer_->Append(
      {reinterpret_cast<const std::uint8_t*>(key.data()), key.size()},
      {reinterpret_cast<const std::uint8_t*>(value.data()), value.size()},
      [&](Position pos) {
        absl::WriterMutexLock guard(&index_rwlock_);
        index_.insert({key, pos});
      });
}

absl::Status MyBitcask::Delete(const std::string& key) noexcept {
  return log_writer_->AppendTombstone(
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
