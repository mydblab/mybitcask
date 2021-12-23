#include "mybitcask/mybitcask.h"
#include "store_dbfiles.h"

namespace mybitcask {

absl::Span<const std::uint8_t> MakeU8Span(const std::string& s) {
  return {reinterpret_cast<const std::uint8_t*>(s.data()), s.size()};
}

MyBitcask::MyBitcask(std::unique_ptr<store::Store>&& store,
                     log::Reader&& log_reader, log::Writer&& log_writer,
                     absl::btree_map<std::string, Position>&& index)
    : store_(std::move(store)),
      index_(std::move(index)),
      index_rwlock_(),
      log_reader_(std::move(log_reader)),
      log_writer_(std::move(log_writer))

{}

absl::StatusOr<bool> MyBitcask::Get(absl::string_view key, std::string* value,
                                    int try_num) noexcept {
  auto pos = get_position(key);
  if (!pos.has_value()) {
    return false;
  }
  std::uint8_t* value_ptr;
  std::vector<std::uint8_t> trash;
  if (value) {
    value->resize(pos->value_len);
    value_ptr = const_cast<std::uint8_t*>(
        reinterpret_cast<const std::uint8_t*>(value->data()));
  } else {
    trash.resize(pos->value_len);
    value_ptr = trash.data();
  }
  auto found =
      log_reader_.Read(*pos, static_cast<std::uint8_t>(key.size()), value_ptr);

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

absl::StatusOr<std::unique_ptr<MyBitcask>> Open(
    const ghc::filesystem::path& data_dir, std::uint32_t dead_bytes_threshold,
    bool checksum) {
  store::DBFiles dbfiles(data_dir);
  std::unique_ptr<store::Store> store(new store::Store(
      dbfiles.path(), dbfiles.latest_file_id(), dead_bytes_threshold));
  log::Reader log_reader(store.get(), checksum);

  auto index =
      dbfiles.key_iter(&log_reader)
          .Fold<absl::btree_map<std::string, Position>, std::string>(
              absl::btree_map<std::string, Position>(),
              [](absl::btree_map<std::string, Position>&& acc,
                 store::file_id_t file_id, log::Key<std::string>&& key) {
                if (key.value_pos.has_value()) {
                  acc.insert({std::move(key.key_data),
                              Position{file_id, key.value_pos->value_pos,
                                       key.value_pos->value_len}});
                } else {
                  acc.erase(key.key_data);
                }
                return acc;
              });
  if (!index.ok()) {
    return index.status();
  }
  log::Writer log_writer(store.get());
  return std::unique_ptr<MyBitcask>(
      new MyBitcask(std::move(store), std::move(log_reader),
                    std::move(log_writer), std::move(index).value()));
}
}  // namespace mybitcask
