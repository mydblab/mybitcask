#include "log.h"
#include "crc32c/crc32c.h"

namespace mybitcask {
namespace log {
LogReader::LogReader(io::RandomAccessReader* reader) : reader_(reader) {}

absl::StatusOr<std::optional<Entry>> LogReader::Read(uint64_t offset) noexcept {
  // read header
  uint8_t header[kHeaderLen]{};
  auto status_read_len =
      reader_->ReadAt(offset, absl::Span(header, kHeaderLen));
  if (!status_read_len.ok()) {
    return status_read_len.status();
  }
  if (*status_read_len != kHeaderLen) {
    return absl::InternalError(kErrBadEntry);
  }

  auto expected_crc32 = absl::little_endian::Load32(&header[0]);
  const size_t key_size = header[4];
  size_t value_size = absl::little_endian::Load16(&header[5]);
  bool is_tombstone = false;
  if (value_size == kTombstone) {
    is_tombstone = true;
    value_size = 0;
  }

  // read key and value
  auto inner_buf = new uint8_t[kHeaderLen + key_size + value_size];
  auto buf = absl::Span(&inner_buf[kHeaderLen], key_size + value_size);
  auto status_read_len = reader_->ReadAt(offset + kHeaderLen, buf);
  if (!status_read_len.ok()) {
    return status_read_len.status();
  }
  if (*status_read_len != buf.size()) {
    return absl::InternalError(kErrBadEntry);
  }

  // check crc
  auto actual_crc32 = crc32c::Crc32c(inner_buf, kHeaderLen + buf.size());
  if (expected_crc32 != actual_crc32) {
    return absl::InternalError(kErrBadEntry);
  }
  if (is_tombstone) {
    return std::nullopt;
  }
  return std::nullopt;
}
}  // namespace log
}  // namespace mybitcask
