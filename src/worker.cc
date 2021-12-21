#include "worker.h"
#include "mybitcask/internal/store.h"
#include "spdlog/spdlog.h"
#include "store_hint.h"
#include "timer.h"

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

template <class Rep, class Period>
void GenerateHint::Start(const std::chrono::duration<Rep, Period>& interval) {
  stop_fn_ = absl::make_optional(timer::SetInterval(
      [&]() {
        store::LogFiles logfiles(db_path_);
        for (auto log_file_id : logfiles.active_log_files()) {
          auto status = hint_generator_.Generate(log_file_id);
          if (!status.ok()) {
            spdlog::warn(
                "Hint file generation failed. Log file id: {}, status: {}",
                log_file_id, status);
            break;
          }
          spdlog::info("Hint file generated successfully. Log file id: {}",
                       log_file_id);
        }
      },
      interval));
}

}  // namespace worker
}  // namespace mybitcask
