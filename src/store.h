#ifndef MYBITCASK_SRC_STORE_H_
#define MYBITCASK_SRC_STORE_H_

#include "io.h"

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

class Store : public io::RandomAccessReader, public io::SequentialWriter {
 public:
  absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept override;

  absl::StatusOr<std::size_t> ReadAt(
      const Position& pos, absl::Span<std::uint8_t> dst) const noexcept;

  absl::Status Append(absl::Span<const std::uint8_t> src) noexcept override;

  absl::Status Sync() noexcept override;

  Store() = delete;
  Store(file_id_t latest_file_id_, std::filesystem::path path,
        std::function<std::string(file_id_t)> filename_fn,
        std::size_t dead_bytes_threshold);

  ~Store() override;

 private:
  // Get io::RandomAccessReader through the file ID,
  // and return nullptr if the file does not exist
  const io::RandomAccessReader* reader(file_id_t file_id);

  // Latest file id
  file_id_t latest_file_id_;
  // Database file path
  std::filesystem::path path_;
  // Once the current writer exceeds dead_bytes_threshold_ A new file is created
  const std::size_t dead_bytes_threshold_;
  // Function to generate file name by file id
  const std::function<std::string(file_id_t)> filename_fn_;

  // Latest file writer
  io::SequentialWriter* writer_;
  // All readers
  std::unordered_map<file_id_t, const io::RandomAccessReader*> readers_;
};

}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_H_
