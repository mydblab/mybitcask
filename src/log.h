#ifndef MYBITCASK_SRC_LOG_H_
#define MYBITCASK_SRC_LOG_H_

#include "io.h"

#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"

#include <cstdint>
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
const std::string kErrBadValueLength = "value length must be between (0, 65535)";

class Entry;

class LogReader {
 public:
  explicit LogReader(std::unique_ptr<const io::RandomAccessReader>&& src);

  // Initializes this LogReader. it must be called bfore reading log entry.
  // Returns ok status if init successfully. Else return non-ok status
  absl::Status Init() noexcept;

  // Read a log entry at the specified `offset`. Returns ok status and log entry
  // if read successfully. Else return non-ok status
  //
  // Safe for concurrent use by multiple threads.
  absl::StatusOr<absl::optional<Entry>> Read(uint64_t offset) const noexcept;

 private:
  std::unique_ptr<const io::RandomAccessReader> src_;
};

class LogWriter {
 public:
  explicit LogWriter(std::unique_ptr<io::SequentialWriter>&& dest);

  // Initializes this LogReader. it must be called bfore reading log entry.
  // Returns ok status if init successfully. Else return non-ok status
  absl::Status Init() noexcept;

  // Add an log entry to the end of the underlying dest. Returns ok status and
  // the offset of the added entry if append successfully. Else return non-ok
  // status
  //
  // Safe for concurrent use by multiple threads.
  absl::StatusOr<std::uint64_t> Append(
      absl::Span<const std::uint8_t> key,
      absl::Span<const std::uint8_t> value) noexcept;

  // Add a tombstone log entry to the end of the underlying dest. Returns ok
  // status and the offset of the added entry if append successfully. Else
  // return non-ok status
  //
  // A tombstone log entry is a special log entry that indicates that the record
  // corresponding to the `key` once occupied the slot but does so no longer.
  // In other words, delete the record corresponding to this key
  //
  // Safe for concurrent use by multiple threads.
  absl::StatusOr<std::uint64_t> AppendTombstone(
      absl::Span<const std::uint8_t> key) noexcept;

 private:
  absl::StatusOr<std::uint64_t> AppendInner(
      absl::Span<const std::uint8_t> key,
      absl::Span<const std::uint8_t> value) noexcept;

  std::unique_ptr<io::SequentialWriter> dest_;
  std::uint64_t last_append_offset_;
  std::mutex append_lock_;
};

class Entry {
 public:
  Entry(std::uint8_t key_size, std::uint16_t value_size)
      : key_size_(key_size),
        value_size_(value_size),
        ptr_(new uint8_t[static_cast<std::size_t>(key_size) + value_size]) {}

  absl::Span<const std::uint8_t> key() const { return {raw_ptr(), key_size_}; }

  absl::Span<const std::uint8_t> value() const {
    return {raw_ptr() + key_size_, value_size_};
  }

 private:
  const std::uint8_t* raw_ptr() const { return ptr_.get(); }
  std::uint8_t* raw_ptr() { return ptr_.get(); }
  std::size_t raw_buf_size() const {
    return static_cast<std::size_t>(key_size_) + value_size_;
  }

  std::uint8_t key_size_;
  std::uint16_t value_size_;
  std::unique_ptr<std::uint8_t[]> ptr_;

  friend absl::StatusOr<absl::optional<Entry>> LogReader::Read(
      std::uint64_t offset) const noexcept;
};

}  // namespace log
}  // namespace mybitcask

#endif  // MYBITCASK_LOG_H_
