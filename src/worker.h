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

  template <class Rep, class Period>
  void Start(const std::chrono::duration<Rep, Period>& interval);

  ~GenerateHint();

 private:
  ghc::filesystem::path db_path_;
  absl::optional<std::function<void()>> stop_fn_;
  store::hint::Generator hint_generator_;
};

}  // namespace worker
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_WORKER_H_
