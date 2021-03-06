#ifndef MYBITCASK_INCLUDE_INTERNAL_IO_H_
#define MYBITCASK_INCLUDE_INTERNAL_IO_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "ghc/filesystem.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

namespace mybitcask {
namespace io {

// Failed to open file Error
const std::string kErrOpenFailed = "failed open";

// A reader abstraction for reading sequentially bytes
class SequentialReader {
 public:
  SequentialReader() = default;
  SequentialReader(const SequentialReader&) = delete;
  SequentialReader& operator=(const SequentialReader&) = delete;

  virtual ~SequentialReader() = default;

  // Read `dst.size()` bytes from this SequentialReader into the `dst` buffer,
  // returning how many bytes were read. If an error was encountered, a non-OK
  // status will be returned. If non-OK status is returned then it must be
  // guaranteed that no bytes were read.
  //
  // REQUIRES: External synchronization
  virtual absl::StatusOr<std::size_t> Read(
      absl::Span<std::uint8_t> dst) noexcept = 0;

  // Seek to offset `offset`, in bytes, in this reader.
  // If EOF is reached, skipping will stop at the EOF, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual absl::Status Skip(std::uint64_t offset) noexcept = 0;
};

// A reader abstraction for randomly reading bytes.
class RandomAccessReader {
 public:
  RandomAccessReader() = default;
  RandomAccessReader(const RandomAccessReader&) = delete;
  RandomAccessReader& operator=(const RandomAccessReader&) = delete;

  virtual ~RandomAccessReader() = default;

  // Read `dst.size()` bytes from this RandomAccessReader starting at `offset`
  // into the `dst` buffer, returning how many bytes were read. If an error was
  // encountered, a non-OK status will be returned.
  //
  // Safe for concurrent use by multiple threads.
  virtual absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) noexcept = 0;
};

// A writer abstraction for writing sequentially bytes
class SequentialWriter {
 public:
  SequentialWriter() = default;
  SequentialWriter(const SequentialWriter&) = delete;
  SequentialWriter& operator=(const SequentialWriter&) = delete;

  virtual ~SequentialWriter() = default;

  // Write `src.size()` bytes of buffer(`src`) into end of this
  // SequentialWriter. returning the offset before append, If an error was
  // encountered, a non-OK status will be returned. If non-OK status is returned
  // then it must be guaranteed that no bytes were written.
  //
  // REQUIRES: External synchronization
  virtual absl::StatusOr<std::uint32_t> Append(
      absl::Span<const std::uint8_t> src) noexcept = 0;

  // Attempts to sync data to underlying storage. If an error was encountered, a
  // non-OK status will be returned.
  //
  // REQUIRES: External synchronization
  virtual absl::Status Sync() noexcept = 0;

  // Returns the current size of this writer.
  virtual std::uint32_t Size() const noexcept = 0;
};

// Open a file as SequentialWriter
absl::StatusOr<std::unique_ptr<SequentialWriter>> OpenSequentialFileWriter(
    ghc::filesystem::path&& filename) noexcept;

// Open a file as RandomAccessReader
absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessFileReader(
    ghc::filesystem::path&& filename) noexcept;

// Open a file as RandomAccessReader
// Reader using MMAP system call
absl::StatusOr<std::unique_ptr<RandomAccessReader>>
OpenMmapRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept;

// Get the size of the specified file
absl::StatusOr<std::size_t> GetFileSize(
    const ghc::filesystem::path& filename) noexcept;

}  // namespace io
}  // namespace mybitcask
#endif  // MYBITCASK_INCLUDE_INTERNAL_IO_H_
