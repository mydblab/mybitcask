#include "mybitcask/internal/store.h"
#include "store_filename.h"

#include <algorithm>

namespace mybitcask {
namespace store {

Position::Position(file_id_t file_id, std::uint32_t offset_in_file)
    : file_id(file_id), offset_in_file(offset_in_file) {}
Position::Position(const mybitcask::Position& pos)
    : file_id(pos.file_id), offset_in_file(pos.offset_in_file) {}

absl::StatusOr<std::size_t> Store::ReadAt(
    const Position& pos, absl::Span<std::uint8_t> dst) noexcept {
  auto r = reader(pos.file_id);
  if (!r.ok()) {
    return absl::Status(r.status());
  }
  if (*r == nullptr) {
    return absl::OutOfRangeError("Invalid file id");
  }
  return (*r)->ReadAt(pos.offset_in_file, dst);
}

absl::Status Store::Append(
    absl::Span<const uint8_t> src,
    std::function<void(Position)> success_callback) noexcept {
  absl::WriterMutexLock l(&latest_file_lock_);
  if (nullptr == latest_writer_) {
    auto writer =
        io::OpenSequentialFileWriter(path_ / LogFilename(latest_file_id_));
    if (!writer.ok()) {
      return absl::Status(writer.status());
    }
    latest_writer_ = *std::move(writer);
  }
  auto file_size = latest_writer_->Size();

  if (file_size + src.size() > dead_bytes_threshold_) {
    // The current file has exceeded the threshold. Create a new data file.
    auto latest_filename = path_ / LogFilename(++latest_file_id_);
    auto writer =
        io::OpenSequentialFileWriter(ghc::filesystem::path(latest_filename));
    if (!writer.ok()) {
      return absl::Status(writer.status());
    }
    latest_writer_ = *std::move(writer);
    auto reader = io::OpenRandomAccessFileReader(std::move(latest_filename));
    if (!reader.ok()) {
      return absl::Status(reader.status());
    }
    latest_reader_ = *std::move(reader);
  }
  auto offset = latest_writer_->Append(src);
  if (offset.ok()) {
    success_callback(Position(latest_file_id_, *offset));
  }
  return offset.status();
}

absl::Status Store::Sync() noexcept {
  absl::ReaderMutexLock latest_file_lock(&latest_file_lock_);
  return latest_writer_->Sync();
}

Store::Store(const LogFiles& log_files, std::uint32_t dead_bytes_threshold)
    : latest_file_lock_(),
      path_(log_files.path()),
      dead_bytes_threshold_(dead_bytes_threshold),
      latest_writer_(nullptr),
      readers_(),
      readers_lock_() {
  if (!log_files.active_log_files().empty()) {
    latest_file_id_ = log_files.active_log_files().cend()->file_id;
  } else {
    latest_file_id_ = 1;
  }
}

Store::~Store() { Sync(); }

absl::StatusOr<io::RandomAccessReader*> Store::reader(file_id_t file_id) {
  latest_file_lock_.ReaderLock();
  if (file_id == latest_file_id_) {
    latest_file_lock_.ReaderUnlock();
    latest_file_lock_.Lock();
    if (nullptr == latest_reader_) {
      auto reader =
          io::OpenRandomAccessFileReader(path_ / LogFilename(latest_file_id_));
      if (!reader.ok()) {
        latest_file_lock_.Unlock();
        return absl::Status(reader.status());
      }
      latest_reader_ = *std::move(reader);
    }
    latest_file_lock_.Unlock();
    return latest_reader_.get();
  }
  latest_file_lock_.ReaderUnlock();
  {
    absl::ReaderMutexLock latest_file_lock(&latest_file_lock_);
    readers_lock_.ReaderLock();
    auto it = readers_.find(file_id);
    if (it == readers_.end()) {
      readers_lock_.ReaderUnlock();
      if (file_id > latest_file_id_) {
        return nullptr;
      }
      readers_lock_.Lock();
      it = readers_.find(file_id);
      if (it == readers_.end()) {
        // Read only files can be accessed through MmapRandomAccessFileReader
        // file reader.
        auto r =
            io::OpenMmapRandomAccessFileReader(path_ / LogFilename(file_id));
        if (!r.ok()) {
          readers_lock_.Unlock();
          return absl::Status(r.status());
        }
        it = readers_.emplace(std::move(file_id), *std::move(r)).first;
      }
      readers_lock_.Unlock();
    } else {
      readers_lock_.ReaderUnlock();
    }

    return (*it).second.get();
  }
}

LogFiles::LogFiles(const ghc::filesystem::path& path)
    : path_(path), hint_files_() {
  LogFiles::files_vec_type log_files;
  for (auto const& dir_entry : ghc::filesystem::directory_iterator(path)) {
    file_id_t file_id;
    FileType file_type;
    auto filename = dir_entry.path().filename().string();
    if (ParseFilename(filename, &file_id, &file_type)) {
      switch (file_type) {
        case FileType::kLogFile:
          log_files.emplace_back(LogFiles::Entry{file_id, std::move(filename)});
          break;
        case FileType::kHintFile:
          hint_files_.emplace_back(
              LogFiles::Entry{file_id, std::move(filename)});
          break;
        default:
          break;
      }
    }
  }

  auto comp = [](const LogFiles::Entry& a, const LogFiles::Entry& b) {
    return a.file_id < b.file_id;
  };

  std::sort(log_files.begin(), log_files.end(), comp);
  std::sort(hint_files_.begin(), hint_files_.end(), comp);

  active_log_files_ = LogFiles::files_vec_type(
      log_files.begin(), log_files.begin() + hint_files_.size());
  older_log_files_ = LogFiles::files_vec_type(
      log_files.begin() + hint_files_.size(), log_files.end());
}

}  // namespace store
}  // namespace mybitcask
