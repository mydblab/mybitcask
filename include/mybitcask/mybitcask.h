#ifndef MYBITCASK_INCLUDE_MYBITCASK_H_
#define MYBITCASK_INCLUDE_MYBITCASK_H_

#include "internal/log.h"
#include "internal/store.h"

#include "absl/container/btree_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "ghc/filesystem.hpp"

#include <cstdint>
#include <memory>

namespace mybitcask {

struct Position {
  store::file_id_t file_id;
  std::uint32_t value_pos;
  std::uint16_t value_len;
};

class MyBitcask {
 public:
  MyBitcask(std::unique_ptr<store::Store>&& store, log::Reader&& log_reader,
            log::Writer&& log_writer,
            absl::btree_map<std::string, Position>&& index);

  // If the database contains an entry for `key` store the
  // corresponding value in `value` and return true
  // If there is no entry for "key" leave `value` unchanged and return false
  absl::StatusOr<bool> Get(absl::string_view key, std::string* value,
                           int try_num = 2) noexcept;

  // Writes a key/value pair into store
  absl::Status Insert(const std::string& key,
                      const std::string& value) noexcept;

  absl::Status Delete(const std::string& key) noexcept;

 private:
  absl::optional<Position> get_position(absl::string_view key);

  absl::btree_map<std::string, Position> index_;
  absl::Mutex index_rwlock_;
  std::unique_ptr<store::Store> store_;
  log::Reader log_reader_;
  log::Writer log_writer_;
};

absl::StatusOr<std::unique_ptr<MyBitcask>> Open(
    const ghc::filesystem::path& data_dir, std::uint32_t dead_bytes_threshold,
    bool checksum);

}  // namespace mybitcask

#endif  // MYBITCASK_INCLUDE_MYBITCASK_H_
