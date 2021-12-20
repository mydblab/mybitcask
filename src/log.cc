#include "mybitcask/internal/log.h"

#include "absl/base/internal/endian.h"
#include "assert.h"
#include "crc32c/crc32c.h"
#include "mybitcask/internal/log.h"
#include "mybitcask/mybitcask.h"

#include <cstring>

namespace mybitcask {
namespace log {

// Format of log entry on disk
//
//          |<---------- CRC coverage ----------->|
//              +---- length of ----+
//              |                   |
//              |                   v
// +--------+---+---+---------+-- - - --+-- - - --+
// | CRC32C |key_len| val_len |   key   |   val   |
// +--------+-------+----+----+-- - - --+-- - - --+
//  (32bits) (8bits)  (16bits)               ^
//                       |                   |
//                       +----- size of -----+

// Tombstone is a special mark that indicates that a record once occupied the
// slot but does so no longer. When the `val_len` value in the log entry is
// kTombstone(0xFFFF), it means delete the record corresponding to the key. In
// this case the `val` in this log entry is empty
const std::uint16_t kTombstone = 0xFFFF;

const std::uint32_t kCrc32Len = 4;
const std::uint32_t kKeyLenLen = 1;
const std::uint32_t kValLenLen = 2;
const std::uint32_t kHeaderLen = kCrc32Len + kKeyLenLen + kValLenLen;

// RawHeader represents log entry header which contains CRC32C, key_size and
// value_size
class RawHeader final {
 public:
  RawHeader(std::uint8_t* const data) : data_(data) {}

  std::uint8_t key_len() const { return data_[kCrc32Len]; }
  std::uint16_t value_len() const {
    return is_tombstone() ? 0 : raw_value_len();
  }
  bool is_tombstone() const { return raw_value_len() == kTombstone; }

  void set_crc32(std::uint32_t crc32) {
    absl::little_endian::Store32(&data_[0], crc32);
  }
  void set_key_len(std::uint8_t key_len) { data_[kCrc32Len] = key_len; }
  void set_value_len(std::uint16_t value_len) {
    absl::little_endian::Store16(&data_[kCrc32Len + kKeyLenLen], value_len);
  }
  void set_tombstone() { set_value_len(kTombstone); }

  std::uint8_t* data() { return data_; }

  // Returns true if the crc is correct, false if it's incorrect
  bool check_crc(const std::uint8_t* kv_data) const {
    return crc32() == calc_actual_crc(kv_data);
  }

  // Calculate crc32 from key_size, value_size and kv_buf
  std::uint32_t calc_actual_crc(const std::uint8_t* kv_data) const {
    auto header_crc =
        crc32c::Crc32c(&data_[kCrc32Len], kKeyLenLen + kValLenLen);
    return crc32c::Extend(header_crc, kv_data,
                          static_cast<std::size_t>(key_len()) + value_len());
  }

 private:
  std::uint16_t raw_value_len() const {
    return absl::little_endian::Load16(&data_[kCrc32Len + kKeyLenLen]);
  }

  std::uint32_t crc32() const { return absl::little_endian::Load32(&data_[0]); }

  std::uint8_t* const data_;
};

Entry::Entry(std::uint32_t length)
    : ptr_(new uint8_t[length]), key_len_(0), value_len_(0) {}

absl::Span<const std::uint8_t> Entry::key() const {
  return {raw_ptr() + kHeaderLen, key_len_};
}

absl::Span<const std::uint8_t> Entry::value() const {
  return {raw_ptr() + kHeaderLen + key_len_, value_len_};
}

Reader::Reader(store::Store* src, bool checksum)
    : src_(src), checksum_(checksum) {}

absl::StatusOr<bool> Reader::Read(const Position& pos, std::uint8_t key_len,
                                  std::uint8_t* value) noexcept {
  if (checksum_) {
    auto entry = Read(pos, key_len);
    if (!entry.ok()) {
      return entry.status();
    }
    if (!entry->has_value()) {
      return false;
    }
    if (value) {
      auto entry_val = (*entry)->value();
      std::memcpy(value, entry_val.data(), entry_val.length());
    }
    return true;
  }

  auto dst = absl::MakeSpan(value, pos.value_len);
  auto read_len =
      src_->ReadAt(store::Position(pos.file_id, pos.value_pos), dst);
  if (!read_len.ok()) {
    return read_len.status();
  }
  if (*read_len == 0) {
    // entry does not exist
    return false;
  }
  if (*read_len != dst.length()) {
    return absl::InternalError(kErrBadEntry);
  }
  return true;
}

absl::StatusOr<absl::optional<Entry>> Reader::Read(
    const Position& pos, std::uint8_t key_len) noexcept {
  std::uint32_t entry_len = kHeaderLen + key_len + pos.value_len;
  Entry entry(entry_len);

  auto read_len = src_->ReadAt(
      store::Position(pos.file_id, pos.value_pos - key_len - kHeaderLen),
      {entry.raw_ptr(), entry_len});
  if (!read_len.ok()) {
    return read_len.status();
  }
  if (*read_len == 0) {
    // entry does not exist
    return absl::nullopt;
  }
  if (*read_len != entry_len) {
    return absl::InternalError(kErrBadEntry);
  }
  RawHeader header(entry.raw_ptr());
  entry.set_key_len(header.key_len());
  entry.set_value_len(header.value_len());

  // check crc
  if (checksum_ && !header.check_crc(entry.key().data())) {
    return absl::InternalError(kErrBadEntry);
  }
  if (header.is_tombstone()) {
    return absl::nullopt;
  }
  return entry;
}

KeyIter Reader::key_iter(store::file_id_t start_file_id,
                         store::file_id_t end_file_id) const {
  return KeyIter(src_, start_file_id, end_file_id);
}

Writer::Writer(store::Store* dest) : dest_(dest) {}

absl::Status Writer::AppendTombstone(
    absl::Span<const std::uint8_t> key,
    std::function<void(Position)> success_callback) noexcept {
  return AppendInner(key, {}, success_callback);
}

absl::Status Writer::Append(
    absl::Span<const std::uint8_t> key, absl::Span<const std::uint8_t> value,
    std::function<void(Position)> success_callback) noexcept {
  if (value.size() == 0) {
    return absl::InternalError(kErrBadValueLength);
  }
  return AppendInner(key, value, success_callback);
}

absl::Status Writer::AppendInner(
    absl::Span<const std::uint8_t> key, absl::Span<const std::uint8_t> value,
    std::function<void(Position)> success_callback) noexcept {
  if (key.size() == 0 || key.size() > 0xFF) {
    return absl::InternalError(kErrBadKeyLength);
  }
  if (value.size() >= kTombstone) {
    return absl::InternalError(kErrBadValueLength);
  }

  auto key_len = static_cast<std::uint8_t>(key.size());
  auto value_len = static_cast<std::uint16_t>(value.size());

  std::uint32_t buf_len = kHeaderLen + key_len + value_len;
  std::unique_ptr<std::uint8_t[]> buf(new std::uint8_t[buf_len]);

  RawHeader header(buf.get());
  header.set_key_len(key_len);
  std::memcpy(&buf[kHeaderLen], key.data(), key_len);

  if (value.size() > 0) {
    header.set_value_len(value_len);
    std::memcpy(&buf[kHeaderLen + key.size()], value.data(), value_len);
  } else {
    header.set_tombstone();
  }

  header.set_crc32(header.calc_actual_crc(&buf[kHeaderLen]));

  auto status = dest_->Append(
      {buf.get(), buf_len}, [=, &success_callback](store::Position pos) {
        success_callback(Position{
            pos.file_id,
            pos.offset_in_file + kHeaderLen + key_len,  // value_pos
            value_len                                   // value_len
        });
      });
  if (!status.ok()) {
    return status;
  }
  status = dest_->Sync();
  if (!status.ok()) {
    return status;
  }
  return absl::OkStatus();
}

KeyIter::KeyIter(store::Store* src, store::file_id_t start_file_id,
                 store::file_id_t end_file_id)
    : src_(src), start_file_id_(start_file_id), end_file_id_(end_file_id) {}

template <typename T>
absl::StatusOr<T> KeyIter::Fold(T init,
                                std::function<T(T&&, KeyIndex&&)> f) noexcept {
  auto acc = std::move(init);
  for (store::file_id_t file_id = start_file_id_; file_id <= end_file_id_;
       file_id++) {
    std::uint32_t offset = 0;
    while (true) {
      // read header
      std::uint8_t header_data[kHeaderLen]{};
      auto read_len = src_->ReadAt(store::Position(file_id, offset),
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
      read_len = src_->ReadAt(store::Position(file_id, offset),
                              absl::MakeSpan(key_data));
      if (!read_len.ok()) {
        return read_len.status();
      }

      if (*read_len != header.key_len()) {
        return absl::InternalError(kErrBadEntry);
      }
      offset += header.key_len();
      acc = f(
          std::move(acc),
          KeyIndex{
              file_id, Key{std::move(key_data),
                           header.is_tombstone() ? absl::nullopt
                                                 : absl::make_optional(ValuePos{
                                                       header.value_len(),
                                                       offset  // value_pos
                                                   })

                       }  // key
          });
      offset += header.value_len();
    }
  }
  return acc;
}

}  // namespace log
}  // namespace mybitcask
