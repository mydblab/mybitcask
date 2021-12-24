#ifndef MYBITCASK_SRC_WORKER_MERGE_H_
#define MYBITCASK_SRC_WORKER_MERGE_H_

#include <chrono>
#include "absl/types/optional.h"
#include "ghc/filesystem.hpp"
#include "mybitcask/internal/log.h"
#include "mybitcask/internal/worker.h"
#include "spdlog/spdlog.h"
#include "store_dbfiles.h"
#include "store_filename.h"
#include "store_hint.h"
#include "timer.h"

namespace mybitcask {
namespace worker {

template <typename Container>
class Merge final : public Worker {
 public:
  Merge(log::Reader* log_reader, const ghc::filesystem::path& db_path,
        float merge_threshold,
        std::function<bool(const log::Key<Container>&)>&& key_valid_fn,
        std::function<absl::Status(log::Key<Container>&&)>&& re_insert_fn)
      : db_path_(db_path),
        stop_fn_(absl::nullopt),
        merge_threshold_(merge_threshold),
        merger_(store::hint::Merger<Container>(log_reader, db_path,
                                               std::move(key_valid_fn),
                                               std::move(re_insert_fn))){};

  void Start(std::size_t interval_seconds) {
    stop_fn_ = absl::make_optional(timer::SetInterval(
        [&]() {
          store::DBFiles dbfiles(db_path_);
          auto hint_files = dbfiles.hint_files();

          for (auto file_id : hint_files.subspan(0, hint_files.size() - 1)) {
            auto data_distribution = merger_.DataDistribution(file_id);
            if (!data_distribution.ok()) {
              spdlog::warn(
                  "Failed to merge file. Unable to get datadistribution, "
                  "file "
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

  ~Merge() {
    if (stop_fn_.has_value()) {
      stop_fn_.value()();
    }
  }

 private:
  ghc::filesystem::path db_path_;
  absl::optional<std::function<void()>> stop_fn_;
  float merge_threshold_;
  store::hint::Merger<Container> merger_;
};

}  // namespace worker
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_WORKER_MERGE_H_
