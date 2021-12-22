#ifndef MYBITCASK_SRC_STORE_DBFILES_H_
#define MYBITCASK_SRC_STORE_DBFILES_H_

#include "absl/types/span.h"
#include "mybitcask/internal/log.h"
#include "store_hint.h"

namespace mybitcask {
namespace store {

class KeyIter;

class DBFiles {
 public:
  DBFiles(const ghc::filesystem::path& path);

  const ghc::filesystem::path& path() const { return path_; }

  absl::Span<const file_id_t> active_log_files() const {
    return absl::MakeSpan(active_log_files_);
  }
  absl::Span<const file_id_t> older_log_files() const {
    return absl::MakeSpan(older_log_files_);
  }

  file_id_t latest_file_id() const;

  absl::Span<const file_id_t> hint_files() const {
    return absl::MakeSpan(hint_files_);
  }

  KeyIter key_iter(const log::Reader* log_reader) const;

 private:
  ghc::filesystem::path path_;
  std::vector<file_id_t> active_log_files_;
  std::vector<file_id_t> older_log_files_;
  std::vector<file_id_t> hint_files_;
};

struct Void {};

class KeyIter {
 public:
  KeyIter(const log::Reader* log_reader, const ghc::filesystem::path* path,
          const std::vector<file_id_t>* hint_files,
          const std::vector<file_id_t>* active_log_files);

  template <typename T>
  absl::StatusOr<T> Fold(T init,
                         std::function<T(T&&, log::Key&&)> f) const noexcept {
    auto&& acc = std::move(init);
    for (auto& hint_file_id : *hint_files_) {
      auto status = hint::KeyIter(path_, hint_file_id)
                        .Fold<Void>(Void(), [&](Void&&, log::Key&& key) {
                          acc = f(std::move(acc), std::move(key));
                          return Void();
                        });
      if (!status.ok()) {
        return status;
      }
    }

    for (auto& log_file_id : *active_log_files_) {
      auto status = log_reader_->key_iter(log_file_id)
                        .Fold<Void>(Void(), [&](Void&& _, log::Key&& key) {
                          acc = f(std::move(acc), std::move(key));
                          return Void();
                        });
      if (!status.ok()) {
        return status;
      }
    }
    return acc;
  }

 private:
  const log::Reader* log_reader_;
  const ghc::filesystem::path* path_;
  const std::vector<file_id_t>* hint_files_;
  const std::vector<file_id_t>* active_log_files_;
};

}  // namespace store
}  // namespace mybitcask
#endif  // MYBITCASK_SRC_STORE_DBFILES_H_
