#include "mybitcask/mybitcask.h"

namespace mybitcask {

MyBitcask::MyBitcask() : index_(), index_rwlock_() {}

absl::StatusOr<bool> MyBitcask::Get(absl::string_view key,
                                    std::string* value) noexcept {
  // TODO: retry
  auto pos = get_position(key);
  if (!pos.has_value()) {
    return false;
  }
  auto entry_opt = log_reader_->Read(log::Position{pos->offset, pos->length});
  if (!entry_opt.ok()) {
    return entry_opt.status();
  }
  if (!entry_opt->has_value()) {
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
      [&](log::Position pos) {
        absl::WriterMutexLock guard(&index_rwlock_);
        index_.insert({key, Position{pos.offset, pos.length}});
      });
}

absl::Status MyBitcask::Delete(const std::string& key) noexcept {
  return log_writer_->AppendTombstone(
      {reinterpret_cast<const std::uint8_t*>(key.data()), key.size()},
      [&](log::Position) {
        absl::WriterMutexLock guard(&index_rwlock_);
        index_.erase(key);
      });
}

absl::optional<MyBitcask::Position> MyBitcask::get_position(
    absl::string_view key) {
  absl::ReaderMutexLock guard(&index_rwlock_);
  auto const search = index_.find(key);
  if (search == index_.cend()) {
    return absl::nullopt;
  }
  return search->second;
}
}  // namespace mybitcask