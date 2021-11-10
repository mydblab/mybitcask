#ifndef MYBITCASK_SRC_STORE_H_
#define MYBITCASK_SRC_STORE_H_

#include "absl/synchronization/mutex.h"
#include "ghc/filesystem.hpp"
#include "io.h"

#include <unordered_map>

namespace mybitcask {
namespace store {

using file_id_t = uint32_t;

// Position represents a position in db files.
struct Position {
  file_id_t file_id;
  uint64_t offset_in_file;
};

class Store : public io::RandomAccessReader, public io::SequentialWriter {
 public:
  absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) noexcept override;

  absl::StatusOr<std::size_t> ReadAt(const Position& pos,
                                     absl::Span<std::uint8_t> dst) noexcept;

  absl::Status Append(absl::Span<const std::uint8_t> src) noexcept override;

  absl::Status Sync() noexcept override;

  absl::StatusOr<std::uint64_t> Size() const noexcept override;

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
  std::unique_ptr<SequentialWriter> latest_writer_;
  // Latest file reader
  std::unique_ptr<RandomAccessReader> latest_reader_;
  absl::Mutex latest_file_lock_;

  // Database file path
  ghc::filesystem::path path_;
  // Once the current writer exceeds dead_bytes_threshold_ A new file is created
  const std::size_t dead_bytes_threshold_;
  // Function to generate file name by file id
  const std::function<std::string(file_id_t)> filename_fn_;

  // All readers
  std::unordered_map<file_id_t, std::unique_ptr<RandomAccessReader>> readers_;
  absl::Mutex readers_lock_;
};

std::unique_ptr<Store> Open(const ghc::filesystem::path& path,
                            std::size_t dead_bytes_threshold);
}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_H_
