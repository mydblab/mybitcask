#include "worker.h"
#include "ghc/filesystem.hpp"
#include "mybitcask/internal/store.h"
#include "store_dbfiles.h"
#include "store_filename.h"
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
        for (auto log_file_id :
             active_log_files.subspan(0, active_log_files.size() - 1)) {
          auto status = hint_generator_.Generate(log_file_id);
          if (!status.ok()) {
            spdlog::warn(
                "Hint file generation failed. Log file id: {}, status: {}",
                log_file_id, status.ToString());
            break;
          }
          spdlog::info("Hint file generated successfully. Log file id: {}",
                       log_file_id);
        }
      },
      std::chrono::seconds(interval_seconds)));
}

Merge::Merge(log::Reader* log_reader, const ghc::filesystem::path& db_path,
             float merge_threshold,
             std::function<bool(const log::Key&)>&& key_valid_fn,
             std::function<absl::Status(const log::Key&&)>&& re_insert_fn)
    : db_path_(db_path),
      stop_fn_(absl::nullopt),
      merge_threshold_(merge_threshold),
      merger_(store::hint::Merger(log_reader, db_path, std::move(key_valid_fn),
                                  std::move(re_insert_fn))) {}

Merge::~Merge() {
  if (stop_fn_.has_value()) {
    stop_fn_.value()();
  }
}

void Merge::Start(std::size_t interval_seconds) {
  stop_fn_ = absl::make_optional(timer::SetInterval(
      [&]() {
        store::DBFiles dbfiles(db_path_);
        auto hint_files = dbfiles.hint_files();

        for (auto file_id : hint_files.subspan(0, hint_files.size() - 1)) {
          auto data_distribution = merger_.DataDistribution(file_id);
          if (!data_distribution.ok()) {
            spdlog::warn(
                "Failed to merge file. Unable to get datadistribution, file "
                "id: "
                "{}, status: {}",
                file_id, data_distribution.status().ToString());
            break;
          }
          if (static_cast<float>((*data_distribution).valid_data_len) /
                  static_cast<float>((*data_distribution).total_data_len) <=
              merge_threshold_) {
            auto status = merger_.Merge(file_id);
            if (!status.ok()) {
              spdlog::warn("Merge file failed. file id: {}, status: {}",
                           file_id, status.ToString());
              break;
            }
            spdlog::info("Merge file successfully. file id: {}", file_id);

            ghc::filesystem::remove(db_path_ / store::HintFilename(file_id));
            ghc::filesystem::remove(db_path_ / store::LogFilename(file_id));
          }
        }
      },
      std::chrono::seconds(interval_seconds)));
}
}  // namespace worker
}  // namespace mybitcask
