#include "store_hint.h"
#include "store_filename.h"

#include <fstream>

namespace mybitcask {
namespace store {

namespace hint {

// If the value of the `value_len` field in the `Entry` structure is
// kTombstone(0xFFFF) then this record is a tombstone record
const std::uint16_t kTombstone = 0xFFFF;

const std::uint32_t kKeyLenLen = 1;
const std::uint32_t kValLenLen = 2;
const std::uint32_t kValPosLen = 4;
const std::uint32_t kHeaderLen = kKeyLenLen + kValLenLen + kValPosLen;

class RawHeader final {
 public:
  RawHeader(std::uint8_t* const data) : data_(data) {}

  std::uint8_t key_len() const { return data_[0]; }
  std::uint16_t value_len() const {
    return is_tombstone() ? 0 : raw_value_len();
  }
  std::uint32_t value_pos() const {
    return absl::little_endian::Load32(&data_[kKeyLenLen + kValLenLen]);
  }
  bool is_tombstone() const { return raw_value_len() == kTombstone; }
  void set_key_len(std::uint8_t key_len) { data_[0] = key_len; }
  void set_value_len(std::uint16_t value_len) {
    absl::little_endian::Store16(&data_[kKeyLenLen], value_len);
  }
  void set_value_pos(std::uint32_t value_pos) const {
    absl::little_endian::Store32(&data_[kKeyLenLen + kValLenLen], value_pos);
  }
  void set_tombstone() { set_value_len(kTombstone); }

 private:
  std::uint16_t raw_value_len() const {
    return absl::little_endian::Load16(&data_[kKeyLenLen]);
  }
  std::uint8_t* const data_;
};

Generator::Generator(log::Reader* log_reader, const ghc::filesystem::path& path)
    : log_reader_(log_reader), path_(path) {}

struct Void {};

absl::Status Generator::Generate(std::uint32_t file_id) noexcept {
  auto log_file_path = path_ / LogFilename(file_id);

  if (!ghc::filesystem::exists(log_file_path)) {
    return absl::NotFoundError(kErrLogFileNotExist);
  }

  std::ifstream log_file(log_file_path, std::ios::binary | std::ios::in);
  if (!log_file.is_open()) {
    return absl::InternalError(io::kErrOpenFailed);
  }

  std::ofstream hint_file(path_ / HintFilename(file_id),
                          std::ios::binary | std::ios::trunc | std::ios::out);
  if (!hint_file) {
    return absl::InternalError(kErrRead);
  }

  log_reader_->key_iter(file_id, file_id)
      .Fold<Void>(Void(), [&](Void&&, log::Key&& key) { return Void(); });

  /*
    std::uint8_t header_data[kHeaderLen]{};
        RawHeader header(header_data);
        // write header
        hint_file.write(reinterpret_cast<char*>(header_data), kHeaderLen);

        if (hint_file.eof()) {
          return acc;
        }
        if (hint_file.fail()) {
          return absl::InternalError(kErrRead);
        }

  */
  return absl::Status();
}

template <typename T>
absl::StatusOr<T> FoldKeys(absl::string_view hint_filepath, T init,
                           std::function<T(T&&, Entry&&)> f) noexcept {
  std::ifstream file(hint_filepath.data(), std::ios::binary | std::ios::in);

  if (!file) {
    return absl::InternalError(kErrRead);
  }
  auto acc = std::move(init);
  while (true) {
    std::uint8_t header_data[kHeaderLen]{};
    file.read(header_data, kHeaderLen);
    if (file.eof()) {
      return acc;
    }
    if (file.fail()) {
      return absl::InternalError(kErrRead);
    }
    RawHeader header(header_data);
    Entry entry{};
    entry.value_pos = header.is_tombstone() ? absl::nullopt
                                            : EntryValuePos{header.value_len(),
                                                            header.value_pos()};
    entry.key.resize(header.key_len());
    file.read(reinterpret_cast<char*>(entry.key.data()), header.key_len());
    if (file.fail()) {
      return absl::InternalError(kErrRead);
    }
    acc = f(std::move(acc), std::move(entry));
  }
  return acc;
}

}  // namespace hint
}  // namespace store
}  // namespace mybitcask
