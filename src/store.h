#ifndef MYBITCASK_SRC_STORE_H_
#define MYBITCASK_SRC_STORE_H_

#include "io.h"

#include <cstddef>
#include <filesystem>
#include <unordered_map>

namespace mybitcask {
namespace store {

using file_id_t = uint32_t;

// Position represents a position in db files.
struct Position {
  file_id_t file_id;
  uint64_t offset_in_file;
};

// default dead bytes threshold (128MB)
const size_t kDefaultDeadBytesThreshold = 128 * 1024 * 1024;

class Reader;

class Store : public io::RandomAccessReader, public io::SequentialWriter {
 public:
  absl::StatusOr<size_t> ReadAt(uint64_t offset, size_t n,
                                uint8_t* dst) noexcept override;

  absl::StatusOr<size_t> ReadAt(const Position& pos, size_t n,
                                uint8_t* dst) noexcept;

  absl::Status Append(size_t n, uint8_t* src) noexcept override;

  absl::Status Sync() noexcept override;

  Store() = delete;
  Store(file_id_t latest_file_id_, std::filesystem::path path,
        std::function<std::string(file_id_t)> filename_fn,
        size_t dead_bytes_threshold);

  ~Store() override;

 private:
  // Get io::RandomAccessReader through the file ID,
  // and return nullptr if the file does not exist
  io::RandomAccessReader* reader(file_id_t file_id);

  // Latest file id
  file_id_t latest_file_id_;
  // Database file path
  std::filesystem::path path_;
  // Once the current writer exceeds dead_bytes_threshold_ A new file is created
  const size_t dead_bytes_threshold_;
  // Function to generate file name by file id
  const std::function<std::string(file_id_t)> filename_fn_;

  // Latest file writer
  io::SequentialWriter* writer_;
  // All readers
  std::unordered_map<file_id_t, io::RandomAccessReader*> readers_;
};

class PosixMmapRandomAccessReader : public io::RandomAccessReader {
 public:
  PosixMmapRandomAccessReader() = delete;
  ~PosixMmapRandomAccessReader() override;

  static absl::StatusOr<PosixMmapRandomAccessReader*> Open(
      std::string_view filename);

  absl::StatusOr<size_t> ReadAt(uint64_t offset, size_t n,
                                uint8_t* dst) noexcept override;

 private:
  PosixMmapRandomAccessReader(uint8_t* mmap_base, size_t length);
  uint8_t* mmap_base_;
  size_t length_;
};

}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_H_
