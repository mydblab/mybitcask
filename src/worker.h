#ifndef MYBITCASK_SRC_WORKER_H_
#define MYBITCASK_SRC_WORKER_H_

#include "absl/types/optional.h"
#include "ghc/filesystem.hpp"
#include "mybitcask/internal/log.h"
#include "store_hint.h"

namespace mybitcask {
namespace worker {

class GenerateHint {
 public:
  GenerateHint(log::Reader* log_reader, const ghc::filesystem::path& db_path);

  void Start(std::size_t interval_seconds);

  ~GenerateHint();

 private:
  ghc::filesystem::path db_path_;
  absl::optional<std::function<void()>> stop_fn_;
  store::hint::Generator hint_generator_;
};

class Merge {
 public:
  Merge(log::Reader* log_reader, const ghc::filesystem::path& db_path,
        float merge_threshold,
        std::function<bool(const log::Key&)>&& key_valid_fn,
        std::function<absl::Status(const log::Key&&)>&& re_insert_fn);

  void Start(std::size_t interval_seconds);

  ~Merge();

 private:
  ghc::filesystem::path db_path_;
  absl::optional<std::function<void()>> stop_fn_;
  float merge_threshold_;
  store::hint::Merger merger_;
};

}  // namespace worker
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_WORKER_H_
