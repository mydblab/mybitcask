#include "store_hint.h"
#include "absl/base/internal/endian.h"
#include "store_filename.h"

namespace mybitcask {
namespace store {

namespace hint {

// If the value of the `value_len` field in the `Entry` structure is
// kTombstone(0xFFFF) then this record is a tombstone record
const std::uint16_t kTombstone = 0xFFFF;

RawHeader::RawHeader(std::uint8_t* const data) : data_(data) {}

std::uint8_t RawHeader::key_len() const { return data_[0]; }
std::uint16_t RawHeader::value_len() const {
  return is_tombstone() ? 0 : raw_value_len();
}

std::uint32_t RawHeader::value_pos() const {
  return absl::little_endian::Load32(&data_[kKeyLenLen + kValLenLen]);
}
bool RawHeader::is_tombstone() const { return raw_value_len() == kTombstone; }
void RawHeader::set_key_len(std::uint8_t key_len) { data_[0] = key_len; }
void RawHeader::set_value_len(std::uint16_t value_len) {
  absl::little_endian::Store16(&data_[kKeyLenLen], value_len);
}
void RawHeader::set_value_pos(std::uint32_t value_pos) {
  absl::little_endian::Store32(&data_[kKeyLenLen + kValLenLen], value_pos);
}
void RawHeader::set_tombstone() { set_value_len(kTombstone); }

std::uint16_t RawHeader::raw_value_len() const {
  return absl::little_endian::Load16(&data_[kKeyLenLen]);
}

Generator::Generator(log::Reader* log_reader, const ghc::filesystem::path& path)
    : log_reader_(log_reader), path_(path) {}

absl::Status Generator::Generate(std::uint32_t file_id) noexcept {
  auto log_file_path = path_ / LogFilename(file_id);

  if (!ghc::filesystem::exists(log_file_path)) {
    return absl::NotFoundError(kErrLogFileNotExist);
  }

  std::ofstream hint_file(path_ / HintFilename(file_id),
                          std::ios::binary | std::ios::trunc | std::ios::out);
  if (!hint_file) {
    return absl::InternalError(kErrRead);
  }

  using key_t = log::Key<std::vector<std::uint8_t>>;
  auto keys =
      log_reader_->key_iter(file_id)
          .Fold<std::vector<key_t>, std::vector<std::uint8_t>>(
              std::vector<key_t>(), [&](std::vector<key_t>&& acc, key_t&& key) {
                acc.push_back(std::move(key));
                return acc;
              });
  if (!keys.ok()) {
    return keys.status();
  }
  for (auto& key : *keys) {
    std::uint8_t header_data[kHeaderLen]{};
    RawHeader header(header_data);
    header.set_key_len(static_cast<std::uint8_t>(key.key_data.size()));
    if (key.value_pos.has_value()) {
      header.set_value_len(key.value_pos->value_len);
      header.set_value_pos(key.value_pos->value_pos);
    } else {
      header.set_tombstone();
    }

    // write header
    hint_file.write(reinterpret_cast<char*>(header_data), kHeaderLen);

    if (hint_file.fail()) {
      return absl::InternalError(kErrWrite);
    }
    hint_file.write(reinterpret_cast<char*>(key.key_data.data()),
                    key.key_data.size());
    if (hint_file.fail()) {
      return absl::InternalError(kErrWrite);
    }
  }
  hint_file.flush();
  return absl::OkStatus();
}

KeyIter::KeyIter(const ghc::filesystem::path* path, file_id_t hint_file_id)
    : path_(path), hint_file_id_(hint_file_id) {}

}  // namespace hint
}  // namespace store
}  // namespace mybitcask
