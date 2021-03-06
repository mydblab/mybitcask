#include "worker_generate_hint.h"
#include "store_dbfiles.h"
#include "timer.h"

namespace mybitcask {
namespace worker {

GenerateHint::GenerateHint(log::Reader* log_reader,
                           const ghc::filesystem::path& db_path,
                           spdlog::logger* logger)
    : db_path_(db_path),
      stop_fn_(absl::nullopt),
      hint_generator_(store::hint::Generator(log_reader, db_path)),
      logger_(logger) {}

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
        for (auto log_file_id :
             active_log_files.subspan(0, active_log_files.size() - 1)) {
          auto status = hint_generator_.Generate(log_file_id);
          if (!status.ok()) {
            logger_->warn(
                "Hint file generation failed. Log file id: {}, status: {}",
                log_file_id, status.ToString());
            break;
          }
          logger_->info("Hint file generated successfully. Log file id: {}",
                       log_file_id);
        }
      },
      std::chrono::seconds(interval_seconds)));
}

}  // namespace worker
}  // namespace mybitcask
