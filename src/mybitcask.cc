#include "mybitcask/mybitcask.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "store_dbfiles.h"
#include "worker_generate_hint.h"
#include "worker_merge.h"

namespace mybitcask {

const std::size_t kWorkerIntervalSeconds = 30;
const std::size_t kSpdlogMaxFileSize = 5 * 1024 * 1024;
const std::size_t kSpdlogMaxFiles = 10;
const std::string kSpdlogFilename = "logs/mybitcask.txt";

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
      log_writer_(std::move(log_writer)),
      generate_hint_worker_(nullptr),
      merge_worker_(nullptr),
      logger_(nullptr) {}

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

void MyBitcask::setup_worker(bool out_log) {
  if (out_log) {
    logger_ = spdlog::rotating_logger_mt(
        "worker", (store_->Path() / kSpdlogFilename).string(),
        kSpdlogMaxFileSize, kSpdlogMaxFiles);

  } else {
    // empty logger
    logger_ = std::shared_ptr<spdlog::logger>(new spdlog::logger("worker"));
  }

  generate_hint_worker_ = std::unique_ptr<worker::Worker>(
      new worker::GenerateHint(&log_reader_, store_->Path(), logger_.get()));
  generate_hint_worker_->Start(kWorkerIntervalSeconds);
  merge_worker_ =
      std::unique_ptr<worker::Worker>(new worker::Merge<std::string>(
          &log_reader_, store_->Path(), 0.2f, logger_.get(),
          [&](const log::Key<std::string>& key) {
            // key_valid_fn
            return key_valid(key);
          },
          [&](log::Key<std::string>&& key) {
            return re_insert(std::move(key));
          }));
  merge_worker_->Start(kWorkerIntervalSeconds);
}

bool MyBitcask::key_valid(const log::Key<std::string>& key) {
  absl::ReaderMutexLock guard(&index_rwlock_);
  auto pos = get_position(absl::string_view(key.key_data));
  if (pos.has_value() && key.value_pos.has_value()) {
    return pos.value().value_pos == key.value_pos.value().value_pos;
  } else if (!pos.has_value() && !key.value_pos.has_value()) {
    return true;
  }
  return false;
}

absl::Status MyBitcask::re_insert(log::Key<std::string>&& key) {
  std::string v;
  auto found = Get(absl::string_view(key.key_data), &v);
  if (!found.ok()) {
    return found.status();
  }
  if (*found) {
    return Insert(key.key_data, v);
  } else {
    return Delete(key.key_data);
  }
}

absl::StatusOr<std::unique_ptr<MyBitcask>> Open(
    const ghc::filesystem::path& data_dir, std::uint32_t dead_bytes_threshold,
    bool checksum, bool out_log) {
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
  auto mybitcask = std::unique_ptr<MyBitcask>(
      new MyBitcask(std::move(store), std::move(log_reader),
                    std::move(log_writer), std::move(index).value()));
  mybitcask->setup_worker(out_log);
  return mybitcask;
}

}  // namespace mybitcask
