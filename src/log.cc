#include "log.h"
#include "crc32c/crc32c.h"

#include <cstring>

namespace mybitcask {
namespace log {

// Format of log entry on disk
//
//          |<--------- CRC coverage ---------->|
//              +---- size of ----+
//              |                 |
//              |                 v
// +--------+---+--+--------+-- - - --+-- - - --+
// | CRC32C |key_sz| val_sz |   key   |   val   |
// +--------+------+----+---+-- - - --+-- - - --+
//  (32bits) (8bits) (16bits)               ^
//                      |                   |
//                      +----- size of -----+

// Tombstone is a special mark that indicates that a record once occupied the
// slot but does so no longer. When the `val_sz` value in the log entry is
// kTombstone(0xFFFF), it means delete the record corresponding to the key. In
// this case the `val` in this log entry is empty
const uint16_t kTombstone = 0xFFFF;

const size_t kCrc32Len = 4;
const size_t kKeySzLen = 1;
const size_t kValSzLen = 2;
const size_t kHeaderLen = kCrc32Len + kKeySzLen + kValSzLen;

LogReader::LogReader(std::unique_ptr<const io::RandomAccessReader> src)
    : src_(std::move(src)) {}

// Header represents log entry header which contains CRC32C, key_size and
// value_size
class Header {
 public:
  Header(std::uint8_t* const data) : data_(data) {}

  std::uint8_t key_size() const { return data_[kCrc32Len]; }
  std::uint8_t value_size() const {
    if (is_tombstone()) {
      return 0;
    }
    return raw_value_size();
  }
  bool is_tombstone() const { return raw_value_size() == kTombstone; }

  void set_crc32(std::uint32_t crc32) {
    absl::little_endian::Store32(&data_[0], crc32);
  }
  void set_key_size(std::uint8_t key_size) { data_[kCrc32Len] = key_size; }
  void set_value_size(std::uint16_t value_size) {
    absl::little_endian::Store32(&data_[kCrc32Len + kKeySzLen], value_size);
  }
  void set_tombstone() { set_value_size(kTombstone); }

  std::uint8_t* data() { return data_; }

  // Returns true if the crc is correct, false if it's incorrect
  bool check_crc(absl::Span<const std::uint8_t> kv_data) const {
    return crc32() == calc_actual_crc(kv_data);
  }

  // Calculate crc32 from key_size, value_size and kv_buf
  std::uint32_t calc_actual_crc(absl::Span<const std::uint8_t> kv_data) const {
    auto header_crc = crc32c::Crc32c(&data_[kCrc32Len], kKeySzLen + kValSzLen);
    return crc32c::Extend(header_crc, kv_data.data(), kv_data.size());
  }

 private:
  std::uint16_t raw_value_size() const {
    return absl::little_endian::Load16(&data_[kCrc32Len + kKeySzLen]);
  }

  std::uint32_t crc32() const { return absl::little_endian::Load32(&data_[0]); }

  std::uint8_t* const data_;
};

absl::StatusOr<std::optional<Entry>> LogReader::Read(
    std::uint64_t offset) noexcept {
  // read header
  uint8_t header_data[kHeaderLen]{};
  Header header(header_data);

  auto status_read_len =
      src_->ReadAt(offset, absl::Span(header.data(), kHeaderLen));
  if (!status_read_len.ok()) {
    return status_read_len.status();
  }
  if (*status_read_len != kHeaderLen) {
    return absl::InternalError(kErrBadEntry);
  }
  auto key_size = header.key_size();
  auto value_size = header.key_size();

  // read key and value
  Entry entry(key_size, value_size);
  auto buf = absl::Span(entry.raw_ptr_, key_size + value_size);
  status_read_len = src_->ReadAt(offset + kHeaderLen, buf);
  if (!status_read_len.ok()) {
    return status_read_len.status();
  }
  if (*status_read_len != buf.size()) {
    return absl::InternalError(kErrBadEntry);
  }

  // check crc
  if (!header.check_crc(buf)) {
    return absl::InternalError(kErrBadEntry);
  }
  if (header.is_tombstone()) {
    return std::nullopt;
  }
  return entry;
}

LogWriter::LogWriter(std::unique_ptr<io::SequentialWriter> dest)
    : dest_(std::move(dest)) {}

absl::Status LogWriter::Append(absl::Span<const std::uint8_t> key,
                               absl::Span<const std::uint8_t> value) {
  std::unique_ptr<uint8_t[]> buf(
      new uint8_t[kHeaderLen + key.size() + value.size()]);
  Header header(buf.get());
  header.set_key_size(key.size());
  std::memcpy(&buf[kHeaderLen], key.data(), key.size());
  if (value.size() >= 0) {
    header.set_value_size(value.size());
    std::memcpy(&buf[kHeaderLen + key.size()], value.data(), value.size());
  } else {
    header.set_tombstone();
  }
  header.set_crc32(
      header.calc_actual_crc(absl::Span(&buf[kHeaderLen], key.size())));
  auto status = dest_->Append(absl::Span(buf.get(), kHeaderLen + key.size()));
  if (!status.ok()) {
    return status;
  }
  return absl::OkStatus();
}

absl::Status LogWriter::AppendTombstone(absl::Span<const std::uint8_t> key) {
  return Append(key, {});
}

}  // namespace log
}  // namespace mybitcask
