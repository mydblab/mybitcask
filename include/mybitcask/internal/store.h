#ifndef MYBITCASK_INUCLDE_INTERNAL_STORE_H_
#define MYBITCASK_INUCLDE_INTERNAL_STORE_H_

#include "io.h"
#include "log.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "ghc/filesystem.hpp"

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace mybitcask {

struct Position;

namespace store {

using file_id_t = std::uint32_t;

// Position represents a position in db files.
struct Position {
  Position(file_id_t file_id, std::uint32_t offset_in_file);

  file_id_t file_id;
  std::uint32_t offset_in_file;
};

class LogFiles {
 public:
  struct KeyEntry {
    file_id_t file_id;
    std::uint32_t offset;
    std::uint32_t length;
    std::vector<std::uint8_t> key;
  };

  LogFiles(const ghc::filesystem::path& path);

  const ghc::filesystem::path& path() const { return path_; }

  const std::vector<file_id_t>& active_log_files() const {
    return active_log_files_;
  }
  const std::vector<file_id_t>& older_log_files() const {
    return older_log_files_;
  }

  const std::vector<file_id_t>& hint_files() const { return hint_files_; }

  template <typename T>
  absl::StatusOr<T> FoldKeys(
      T init, std::function<T(T&&, KeyEntry&&)> f, log::Reader* log_reader) const noexcept;

 private:
  ghc::filesystem::path path_;
  std::vector<file_id_t> active_log_files_;
  std::vector<file_id_t> older_log_files_;
  std::vector<file_id_t> hint_files_;
};

class Store {
 public:
  absl::StatusOr<std::size_t> ReadAt(const Position& pos,
                                     absl::Span<std::uint8_t> dst) noexcept;

  absl::Status Append(
      absl::Span<const std::uint8_t> src,
      std::function<void(Position)> success_callback = [](Position) {
      }) noexcept;

  absl::Status Sync() noexcept;

  Store() = delete;
  Store(const LogFiles& log_files, std::uint32_t dead_bytes_threshold);

  ~Store();

 private:
  // Get io::RandomAccessReader through the file ID,
  // and return nullptr if the file does not exist
  absl::StatusOr<io::RandomAccessReader*> reader(file_id_t file_id);

  // Latest file id
  file_id_t latest_file_id_;
  // Latest file writer
  std::unique_ptr<io::SequentialWriter> latest_writer_;
  // Latest file reader
  std::unique_ptr<io::RandomAccessReader> latest_reader_;
  absl::Mutex latest_file_lock_;

  // Database file path
  ghc::filesystem::path path_;
  // Once the current writer exceeds dead_bytes_threshold_ A new file is created
  const std::size_t dead_bytes_threshold_;

  // All readers
  std::unordered_map<file_id_t, std::unique_ptr<io::RandomAccessReader>>
      readers_;
  absl::Mutex readers_lock_;
};

}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_INUCLDE_INTERNAL_STORE_H_
