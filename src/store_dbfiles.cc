#include "store_dbfiles.h"

namespace mybitcask {
namespace store {

DBFiles::DBFiles(const ghc::filesystem::path& path)
    : path_(path), hint_files_() {
  std::vector<file_id_t> log_files;
  for (auto const& dir_entry : ghc::filesystem::directory_iterator(path)) {
    file_id_t file_id;
    FileType file_type;
    auto filename = dir_entry.path().filename().string();
    if (ParseFilename(filename, &file_id, &file_type)) {
      switch (file_type) {
        case FileType::kLogFile:
          log_files.push_back(file_id);
          break;
        case FileType::kHintFile:
          hint_files_.push_back(file_id);
          break;
        default:
          break;
      }
    }
  }

  std::sort(log_files.begin(), log_files.end());
  std::sort(hint_files_.begin(), hint_files_.end());

  active_log_files_ = std::vector<file_id_t>(
      log_files.begin() + hint_files_.size(), log_files.end());
  older_log_files_ = std::vector<file_id_t>(
      log_files.begin(), log_files.begin() + hint_files_.size());
}

file_id_t DBFiles::latest_file_id() const {
  if (!active_log_files().empty()) {
    return active_log_files().back();
  } else if (!older_log_files().empty()) {
    return older_log_files().back();
  } else {
    return 1;
  }
}

KeyIter DBFiles::key_iter(const log::Reader* log_reader) const {
  return KeyIter(log_reader, &hint_files_, &active_log_files_);
}

KeyIter::KeyIter(const log::Reader* log_reader,
                 const std::vector<file_id_t>* hint_files,
                 const std::vector<file_id_t>* active_log_files)
    : log_reader_(log_reader),
      hint_files_(hint_files),
      active_log_files_(active_log_files) {}

}  // namespace store
}  // namespace mybitcask
