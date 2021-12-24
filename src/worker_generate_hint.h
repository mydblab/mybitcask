#ifndef MYBITCASK_SRC_WORKER_GENERATE_HINT_H_
#define MYBITCASK_SRC_WORKER_GENERATE_HINT_H_

#include "absl/types/optional.h"
#include "mybitcask/internal/log.h"
#include "mybitcask/internal/worker.h"
#include "store_hint.h"
#include "spdlog/spdlog.h"

namespace mybitcask {
namespace worker {

class GenerateHint final : public Worker {
 public:
  GenerateHint(log::Reader* log_reader, const ghc::filesystem::path& db_path, spdlog::logger* logger);

  void Start(std::size_t interval_seconds);

  ~GenerateHint();

 private:
  ghc::filesystem::path db_path_;
  absl::optional<std::function<void()>> stop_fn_;
  store::hint::Generator hint_generator_;
  spdlog::logger* logger_;
};

}  // namespace worker
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_WORKER_GENERATE_HINT_H_
