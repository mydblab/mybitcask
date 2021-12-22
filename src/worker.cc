#include "worker.h"
#include "mybitcask/internal/store.h"
#include "store_dbfiles.h"
#include "store_hint.h"
#include "timer.h"

#include <chrono>
#include "spdlog/spdlog.h"

namespace mybitcask {
namespace worker {

GenerateHint::GenerateHint(log::Reader* log_reader,
                           const ghc::filesystem::path& db_path)
    : db_path_(db_path),
      stop_fn_(absl::nullopt),
      hint_generator_(store::hint::Generator(log_reader, db_path)) {}

GenerateHint::~GenerateHint() {
  if (stop_fn_.has_value()) {
    stop_fn_.value()();
  }
}

void GenerateHint::Start(std::size_t interval_seconds) {
  stop_fn_ = absl::make_optional(timer::SetInterval(
      [&]() {
        store::DBFiles dbfiles(db_path_);
        auto active_log_files = dbfiles.active_log_files();
        for (auto it = active_log_files.begin();
             it != active_log_files.end() - 1; it++) {
          auto status = hint_generator_.Generate(*it);
          if (!status.ok()) {
            spdlog::warn(
                "Hint file generation failed. Log file id: {}, status: {}", *it,
                status.ToString());
            break;
          }
          spdlog::info("Hint file generated successfully. Log file id: {}",
                       *it);
        }
      },
      std::chrono::seconds(interval_seconds)));
}

}  // namespace worker
}  // namespace mybitcask
