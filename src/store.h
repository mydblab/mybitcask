#ifndef MYBITCASK_SRC_STORE_H_
#define MYBITCASK_SRC_STORE_H_

#include "mybitcask/internal/io.h"
#include "mybitcask/mybitcask.h"

#include "absl/synchronization/mutex.h"
#include "ghc/filesystem.hpp"

#include <unordered_map>

namespace mybitcask {
namespace store {

// Position represents a position in db files.
struct Position {
  Position(file_id_t file_id, std::uint32_t offset_in_file)
      : file_id(file_id), offset_in_file(offset_in_file) {}
  Position(const mybitcask::Position& pos)
      : file_id(pos.file_id), offset_in_file(pos.offset_in_file) {}

  file_id_t file_id;
  std::uint32_t offset_in_file;
};

class Store {
 public:
  absl::StatusOr<std::size_t> ReadAt(const Position& pos,
                                     absl::Span<std::uint8_t> dst) noexcept;

  absl::Status Append(
      absl::Span<const std::uint8_t> src,
      std::function<void(Position)> success_callback = [](Position) {}) noexcept;

  absl::Status Sync() noexcept;

  absl::StatusOr<std::uint64_t> Size() const noexcept;

  Store() = delete;
  Store(file_id_t latest_file_id_, ghc::filesystem::path path,
        std::function<std::string(file_id_t)> filename_fn,
        std::size_t dead_bytes_threshold);

  ~Store() = default;

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
  // Function to generate file name by file id
  const std::function<std::string(file_id_t)> filename_fn_;

  // All readers
  std::unordered_map<file_id_t, std::unique_ptr<io::RandomAccessReader>>
      readers_;
  absl::Mutex readers_lock_;
};

std::unique_ptr<Store> Open(const ghc::filesystem::path& path,
                            std::size_t dead_bytes_threshold);
}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_H_
