#ifndef MYBITCASK_INCLUDE_INTERNAL_LOG_H_
#define MYBITCASK_INCLUDE_INTERNAL_LOG_H_

#include "store.h"

#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace mybitcask {

struct Position;

namespace log {

// kErrBadEntry is an error. that indicates log entry is invalid.
// Currently there are two situations in which this error will occur:
// * The entry has an invalid CRC
// * The entry has wrong length
const std::string kErrBadEntry = "bad log entry";
const std::string kErrBadKeyLength = "key length must be between (0, 255]";
const std::string kErrBadValueLength =
    "value length must be between (0, 65535)";

const std::uint32_t kCrc32Len = 4;
const std::uint32_t kKeyLenLen = 1;
const std::uint32_t kValLenLen = 2;
const std::uint32_t kHeaderLen = kCrc32Len + kKeyLenLen + kValLenLen;

// RawHeader represents log entry header which contains CRC32C, key_size and
// value_size
class RawHeader final {
 public:
  RawHeader(std::uint8_t* const data);

  std::uint8_t key_len() const;
  std::uint16_t value_len() const;
  bool is_tombstone() const;
  void set_crc32(std::uint32_t crc32);
  void set_key_len(std::uint8_t key_len);
  void set_value_len(std::uint16_t value_len);
  void set_tombstone();
  std::uint8_t* data();

  // Returns true if the crc is correct, false if it's incorrect
  bool CheckCrc(const std::uint8_t* kv_data) const;

  // Calculate crc32 from key_size, value_size and kv_buf
  std::uint32_t calc_actual_crc(const std::uint8_t* kv_data) const;

 private:
  std::uint16_t raw_value_len() const;

  std::uint32_t crc32() const;

  std::uint8_t* const data_;
};

class Entry;

class KeyIter;

class Reader {
 public:
  Reader() = default;

  // Create LogReader
  // If "checksum" is true, verify checksums if available.
  Reader(store::Store* src, bool checksum);

  // Read a log entry at the specified position.
  // Return true and store value part of entry to `value` if read successfully.
  // Else return false.
  // If checksum is not required, write the corresponding value directly to
  // `value`. Else read out the entire entry first, and then copy the value part
  // of the entry to value.
  //
  // Safe for concurrent use by multiple threads.
  absl::StatusOr<bool> Read(const Position& pos, std::uint8_t key_len,
                            std::uint8_t* value) noexcept;

  // Read a log entry a t the specified position. Returns ok status and log
  // entry if read successfully. Else return non-ok status
  //
  // Safe for concurrent use by multiple threads.
  absl::StatusOr<absl::optional<Entry>> Read(const Position& pos,
                                             std::uint8_t key_len) noexcept;

  // Returns an key iterator
  KeyIter key_iter(store::file_id_t log_file_id) const;

 private:
  store::Store* src_;
  bool checksum_;
};

class Writer {
 public:
  Writer() = default;

  explicit Writer(store::Store* dest);

  // Add an log entry to the end of the underlying dest. Returns ok status and
  // the offset and length of the added entry if append successfully. Else
  // return non-ok status
  //
  // Safe for concurrent use by multiple threads.
  absl::Status Append(
      absl::Span<const std::uint8_t> key, absl::Span<const std::uint8_t> value,
      const std::function<void(Position)>& success_callback) noexcept;

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
      const std::function<void(Position)>& success_callback) noexcept;

 private:
  absl::Status AppendInner(
      absl::Span<const std::uint8_t> key, absl::Span<const std::uint8_t> value,
      const std::function<void(Position)>& success_callback) noexcept;

  store::Store* dest_;
};

class Entry final {
 public:
  Entry(std::uint32_t length);

  absl::Span<const std::uint8_t> key() const;

  absl::Span<const std::uint8_t> value() const;

 private:
  const std::uint8_t* raw_ptr() const { return ptr_.get(); }
  std::uint8_t* raw_ptr() { return ptr_.get(); }

  void set_key_len(std::uint8_t key_len) { key_len_ = key_len; }
  void set_value_len(std::uint16_t value_len) { value_len_ = value_len; }

  std::uint8_t key_len_;
  std::uint16_t value_len_;
  std::unique_ptr<std::uint8_t[]> ptr_;

  friend absl::StatusOr<absl::optional<Entry>> Reader::Read(
      const Position& pos, std::uint8_t key_len) noexcept;
};

struct ValuePos {
  std::uint16_t value_len;
  std::uint32_t value_pos;
};

struct Key {
  std::vector<std::uint8_t> key_data;
  // value_pos if empty means tombstone entry
  absl::optional<ValuePos> value_pos;
};

class KeyIter {
 public:
  KeyIter(store::Store* src, store::file_id_t log_file_id);

  // Folds keys into an accumulator by applying an operation, returning the
  // final result.
  template <typename T>
  absl::StatusOr<T> Fold(T init, std::function<T(T&&, Key&&)> f) noexcept {
    auto&& acc = std::move(init);

    std::uint32_t offset = 0;
    while (true) {
      // read header
      std::uint8_t header_data[kHeaderLen]{};
      auto read_len = src_->ReadAt(store::Position(log_file_id_, offset),
                                   {header_data, kHeaderLen});
      if (!read_len.ok()) {
        return read_len.status();
      }
      if (*read_len == 0) {
        // end of file
        break;
      }
      if (*read_len != kHeaderLen) {
        return absl::InternalError(kErrBadEntry);
      }
      RawHeader header(header_data);

      // read key
      offset += kHeaderLen;
      std::vector<std::uint8_t> key_data(header.key_len());
      read_len = src_->ReadAt(store::Position(log_file_id_, offset),
                              absl::MakeSpan(key_data));
      if (!read_len.ok()) {
        return read_len.status();
      }

      if (*read_len != header.key_len()) {
        return absl::InternalError(kErrBadEntry);
      }
      offset += header.key_len();
      acc = f(std::move(acc),
              Key{std::move(key_data), header.is_tombstone()
                                           ? absl::nullopt
                                           : absl::make_optional(ValuePos{
                                                 header.value_len(),
                                                 offset  // value_pos
                                             })

              });
      offset += header.value_len();
    }
    return acc;
  }

 private:
  store::Store* src_;
  store::file_id_t log_file_id_;
};

}  // namespace log
}  // namespace mybitcask

#endif  // MYBITCASK_INCLUDE_INTERNAL_LOG_H_
