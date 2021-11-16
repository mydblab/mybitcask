#ifndef MYBITCASK_INCLUDE_INTERNAL_LOG_H_
#define MYBITCASK_INCLUDE_INTERNAL_LOG_H_

#include "mybitcask/mybitcask.h"
#include "store.h"

#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace mybitcask {
namespace log {

// kErrBadEntry is an error. that indicates log entry is invalid.
// Currently there are two situations in which this error will occur:
// * The entry has an invalid CRC
// * The entry has wrong length
const std::string kErrBadEntry = "bad log entry";
const std::string kErrBadKeyLength = "key length must be between (0, 255]";
const std::string kErrBadValueLength =
    "value length must be between (0, 65535)";

class Entry;

class LogReader {
 public:
  // Create LogReader
  // If "checksum" is true, verify checksums if available.
  LogReader(store::Store* src, bool checksum);

  // Read a log entry at the specified position. Returns ok status and log entry
  // if read successfully. Else return non-ok status
  //
  // Safe for concurrent use by multiple threads.
  absl::StatusOr<absl::optional<Entry>> Read(const Position& pos) noexcept;

 private:
  store::Store* src_;
  const bool checksum_;
};

class LogWriter {
 public:
  explicit LogWriter(store::Store* dest);

  // Add an log entry to the end of the underlying dest. Returns ok status and
  // the offset and length of the added entry if append successfully. Else
  // return non-ok status
  //
  // Safe for concurrent use by multiple threads.
  absl::Status Append(absl::Span<const std::uint8_t> key,
                      absl::Span<const std::uint8_t> value,
                      std::function<void(Position)> success_callback) noexcept;

  // Add a tombstone log entry to the end of the underlying dest. Returns ok
  // status and the offset and length of the added entry if append successfully.
  // Else return non-ok status
  //
  // A tombstone log entry is a special log entry that indicates that the record
  // corresponding to the `key` once occupied the slot but does so no longer.
  // In other words, delete the record corresponding to this key
  //
  // Safe for concurrent use by multiple threads.
  absl::Status AppendTombstone(
      absl::Span<const std::uint8_t> key,
      std::function<void(Position)> success_callback) noexcept;

 private:
  absl::Status AppendInner(
      absl::Span<const std::uint8_t> key, absl::Span<const std::uint8_t> value,
      std::function<void(Position)> success_callback) noexcept;

  store::Store* dest_;
};

class Entry final {
 public:
  Entry(std::size_t length);

  absl::Span<const std::uint8_t> key() const;

  absl::Span<const std::uint8_t> value() const;

 private:
  const std::uint8_t* raw_ptr() const { return ptr_.get(); }
  std::uint8_t* raw_ptr() { return ptr_.get(); }

  void set_key_size(std::uint8_t key_size) { key_size_ = key_size; }
  void set_value_size(std::uint16_t value_size) { value_size_ = value_size; }

  std::uint8_t key_size_;
  std::uint16_t value_size_;
  std::unique_ptr<std::uint8_t[]> ptr_;

  friend absl::StatusOr<absl::optional<Entry>> LogReader::Read(
      const Position& pos) noexcept;
};

}  // namespace log
}  // namespace mybitcask

#endif  // MYBITCASK_INCLUDE_INTERNAL_LOG_H_
