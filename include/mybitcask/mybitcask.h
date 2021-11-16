#ifndef MYBITCASK_INCLUDE_MYBITCASK_H_
#define MYBITCASK_INCLUDE_MYBITCASK_H_

#include "internal/log.h"

#include "absl/container/btree_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"

#include <cstdint>

namespace mybitcask {

struct Position {
  std::uint32_t file_id;
  std::uint32_t offset_in_file;
  std::uint32_t length;
};

class MyBitcask {
 public:
  MyBitcask();

  // If the database contains an entry for `key` store the
  // corresponding value in `value` and return true
  // If there is no entry for "key" leave `value` unchanged and return false
  absl::StatusOr<bool> Get(absl::string_view key, std::string* value,
                           int try_num) noexcept;

  // Writes a key/value pair into store
  absl::Status Insert(const std::string& key,
                      const std::string& value) noexcept;

  absl::Status Delete(const std::string& key) noexcept;

 private:
  absl::optional<Position> get_position(absl::string_view key);

  absl::btree_map<std::string, Position> index_;
  absl::Mutex index_rwlock_;
  log::LogReader* log_reader_;
  log::LogWriter* log_writer_;
};
}  // namespace mybitcask

#endif  // MYBITCASK_INCLUDE_MYBITCASK_H_
