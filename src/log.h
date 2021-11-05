#ifndef MYBITCASK_SRC_LOG_H_
#define MYBITCASK_SRC_LOG_H_

#include "absl/status/statusor.h"
#include "io.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace mybitcask {
namespace log {

// kErrBadEntry is an error. that indicates log entry is invalid.
// Currently there are two situations in which this error will occur:
// * The entry has an invalid CRC
// * The entry has wrong length
const std::string kErrBadEntry = "bad entry";

class Entry;

class LogReader {
 public:
  explicit LogReader(std::unique_ptr<const io::RandomAccessReader>&& src);

  // Read a log entry at the specified `offset`. Returns ok status and log entry
  // if read successfully. Else return non-ok status
  absl::StatusOr<std::optional<Entry>> Read(uint64_t offset) noexcept;

 private:
  const std::unique_ptr<const io::RandomAccessReader> src_;
};

class LogWriter {
 public:
  explicit LogWriter(std::unique_ptr<io::SequentialWriter>&& dest);

  // Add an log entry to the end of the underlying dest. Returns ok status if
  // append successfully. Else return non-ok status
  absl::Status Append(absl::Span<const std::uint8_t> key,
                      absl::Span<const std::uint8_t> value) noexcept;

  // Add a tombstone log entry to the end of the underlying dest. Returns ok
  // status if append successfully. Else return non-ok status
  //
  // A tombstone log entry is a special log entry that indicates that the record
  // corresponding to the `key` once occupied the slot but does so no longer.
  // In other words, delete the record corresponding to this key
  absl::Status AppendTombstone(absl::Span<const std::uint8_t> key) noexcept;

 private:
  std::unique_ptr<io::SequentialWriter> dest_;
};

class Entry {
 public:
  Entry(std::uint8_t key_size, std::uint8_t value_size)
      : key_size_(key_size),
        value_size_(value_size),
        raw_ptr_(new uint8_t[key_size + value_size]) {}

  ~Entry() { delete[] raw_ptr_; }

  absl::Span<const std::uint8_t> key() const {
    return absl::Span(raw_ptr_, key_size_);
  }

  absl::Span<const std::uint8_t> value() const {
    return absl::Span(raw_ptr_ + key_size_, value_size_);
  }

 private:
  const std::uint8_t key_size_;
  const std::uint16_t value_size_;
  uint8_t* const raw_ptr_;

  friend absl::StatusOr<std::optional<Entry>> LogReader::Read(
      uint64_t offset) noexcept;
};

}  // namespace log
}  // namespace mybitcask

#endif  // MYBITCASK_LOG_H_
