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

struct DataDistribution {
  std::uint32_t valid_data_len;
  std::uint32_t total_data_len;
};

template <typename Container>
class Merger {
 public:
  Merger(
      log::Reader* log_reader, const ghc::filesystem::path& path,
      std::function<bool(const log::Key<Container>&)>&& key_valid_fn,
      std::function<absl::Status(const log::Key<Container>&&)>&& re_insert_fn)
      : log_reader_(log_reader),
        path_(path),
        key_valid_fn_(std::move(key_valid_fn)),
        re_insert_fn_(std::move(re_insert_fn)){};

  absl::StatusOr<struct DataDistribution> DataDistribution(
      std::uint32_t file_id) noexcept {
    auto keyiter = KeyIter(&path_, file_id);
    return keyiter.template Fold<struct DataDistribution, Container>(
        {0, 0}, [&](struct DataDistribution&& acc, log::Key<Container>&& key) {
          std::uint32_t data_len = key.key_data.size();
          if (key.value_pos.has_value()) {
            data_len += key.value_pos.value().value_len;
          }
          if (key_valid_fn_(key)) {
            acc.valid_data_len += data_len;
          }
          acc.total_data_len += data_len;
          return acc;
        });
  }

  absl::Status Merge(std::uint32_t file_id) noexcept {
    auto keyiter = KeyIter(&path_, file_id);
    auto valid_keys =
        keyiter.template Fold<std::vector<log::Key<Container>>, Container>(
            std::vector<log::Key<Container>>(),
            [&](std::vector<log::Key<Container>>&& acc,
                log::Key<Container>&& key) {
              if (key_valid_fn_(key)) {
                acc.push_back(std::move(key));
              }
              return acc;
            });
    if (!valid_keys.ok()) {
      return valid_keys.status();
    }
    for (auto key : *valid_keys) {
      auto status = re_insert_fn_(std::move(key));
      if (!status.ok()) {
        return status;
      }
    }
    return absl::OkStatus();
  }

 private:
  log::Reader* log_reader_;
  ghc::filesystem::path path_;
  std::function<bool(const log::Key<Container>&)> key_valid_fn_;
  std::function<absl::Status(const log::Key<Container>&&)> re_insert_fn_;
};

class KeyIter {
 public:
  KeyIter(const ghc::filesystem::path* path, file_id_t hint_file_id);

  template <typename T, typename Container>
  absl::StatusOr<T> Fold(
      T init, const std::function<T(T&&, log::Key<Container>&&)>& f) noexcept {
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
      log::Key<Container> key{};
      key.value_pos = header.is_tombstone()
                          ? absl::nullopt
                          : absl::make_optional(log::ValuePos{
                                header.value_len(), header.value_pos()});
      log::log_key_container_internal::resize(key.key_data, header.key_len());
      hint_file.read(
          log::log_key_container_internal::data<Container, char>(key.key_data),
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
