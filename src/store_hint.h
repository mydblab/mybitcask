#ifndef MYBITCASK_SRC_STORE_HINT_H_
#define MYBITCASK_SRC_STORE_HINT_H_

#include "mybitcask/internal/log.h"
#include "mybitcask/internal/store.h"
#include "mybitcask/mybitcask.h"

#include <cstdint>
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
  absl::StatusOr<T> Fold(T init, std::function<T(T&&, log::Key&&)> f) noexcept;

 private:
  const ghc::filesystem::path* path_;
  file_id_t hint_file_id_;
};

}  // namespace hint
}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_HINT_H_
