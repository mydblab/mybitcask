#ifndef MYBITCASK_SRC_STORE_HINT_H_
#define MYBITCASK_SRC_STORE_HINT_H_

#include "mybitcask/internal/log.h"
#include "mybitcask/internal/store.h"
#include "mybitcask/mybitcask.h"
#include "store_filename.h"

#include <cstdint>
#include <fstream>
#include <functional>
#include <vector>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "ghc/filesystem.hpp"

namespace mybitcask {
namespace store {
namespace hint {

const std::string kErrRead = "failed to read hint entry";
const std::string kErrWrite = "failed to write hint entry";
const std::string kErrLogFileNotExist = "log file not exist";

const std::uint32_t kKeyLenLen = 1;
const std::uint32_t kValLenLen = 2;
const std::uint32_t kValPosLen = 4;
const std::uint32_t kHeaderLen = kKeyLenLen + kValLenLen + kValPosLen;


class RawHeader final {
 public:
  RawHeader(std::uint8_t* const data);

  std::uint8_t key_len() const;
  std::uint16_t value_len() const;

  std::uint32_t value_pos() const;
  bool is_tombstone() const;
  void set_key_len(std::uint8_t key_len);
  void set_value_len(std::uint16_t value_len);
  void set_value_pos(std::uint32_t value_pos);
  void set_tombstone();

 private:
  std::uint16_t raw_value_len() const;
  std::uint8_t* const data_;
};

class Generator {
 public:
  Generator(log::Reader* log_reader, const ghc::filesystem::path& path);

  absl::Status Generate(std::uint32_t file_id) noexcept;

 private:
  log::Reader* log_reader_;
  ghc::filesystem::path path_;
};

class KeyIter {
 public:
  KeyIter(const ghc::filesystem::path* path, file_id_t hint_file_id);

  template <typename T>
  absl::StatusOr<T> Fold(T init, std::function<T(T&&, log::Key&&)> f) noexcept {
    std::ifstream hint_file(*path_ / HintFilename(hint_file_id_),
                            std::ios::binary | std::ios::in);
    if (!hint_file) {
      return absl::InternalError(kErrRead);
    }
    auto&& acc = std::move(init);
    while (true) {
      std::uint8_t header_data[kHeaderLen]{};
      hint_file.read(reinterpret_cast<char*>(header_data), kHeaderLen);
      if (hint_file.eof()) {
        return acc;
      }
      if (hint_file.fail()) {
        return absl::InternalError(kErrRead);
      }
      RawHeader header(header_data);
      log::Key key{};
      key.value_pos = header.is_tombstone()
                          ? absl::nullopt
                          : absl::make_optional(log::ValuePos{
                                header.value_len(), header.value_pos()});
      key.key_data.resize(header.key_len());
      hint_file.read(reinterpret_cast<char*>(key.key_data.data()),
                     header.key_len());
      if (hint_file.fail()) {
        return absl::InternalError(kErrRead);
      }
      acc = f(std::move(acc), std::move(key));
    }
    return acc;
  }

 private:
  const ghc::filesystem::path* path_;
  file_id_t hint_file_id_;
};

}  // namespace hint
}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_HINT_H_
