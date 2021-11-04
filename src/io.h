#ifndef MYBITCASK_SRC_IO_H_
#define MYBITCASK_SRC_IO_H_

#include "absl/status/statusor.h"

#include <cstddef>
#include <cstdint>

namespace mybitcask {
namespace io {

// A reader abstraction for reading sequentially bytes
class SequentialReader {
 public:
  SequentialReader(const SequentialReader&) = delete;
  SequentialReader& operator=(const SequentialReader&) = delete;

  virtual ~SequentialReader() = 0;

  // Read `dst.size()` bytes from this SequentialReader into the `dst` buffer,
  // returning how many bytes were read. If an error was encountered, a non-OK
  // status will be returned. If non-OK status is returned then it must be
  // guaranteed that no bytes were read.
  //
  // REQUIRES: External synchronization
  virtual absl::StatusOr<size_t> Read(
      absl::Span<std::uint8_t> dst) noexcept = 0;

  // Seek to offset `offset`, in bytes, in this reader.
  // If EOF is reached, skipping will stop at the EOF, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual absl::StatusOr<size_t> Skip(std::uint64_t offset) noexcept = 0;
};

// A reader abstraction for randomly reading bytes.
class RandomAccessReader {
 public:
  RandomAccessReader(const RandomAccessReader&) = delete;
  RandomAccessReader& operator=(const RandomAccessReader&) = delete;

  virtual ~RandomAccessReader() = 0;

  // Read ``dst.size()` bytes from this RandomAccessReader starting at `offset`
  // into the `dst` buffer, returning how many bytes were read. If an error was
  // encountered, a non-OK status will be returned.
  //
  // Safe for concurrent use by multiple threads.
  virtual absl::StatusOr<size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept = 0;
};

// A writer abstraction for writing sequentially bytes
class SequentialWriter {
 public:
  SequentialWriter(const SequentialWriter&) = delete;
  SequentialWriter& operator=(const SequentialWriter&) = delete;

  virtual ~SequentialWriter() = 0;

  // Write `src.size()` bytes of buffer(`src`) into this SequentialWriter, returning how
  // many bytes were written. If an error was encountered, a non-OK status will
  // be returned. If non-OK status is returned then it must be guaranteed that
  // no bytes were written.
  //
  // Safe for concurrent use by multiple threads.
  virtual absl::StatusOr<size_t> Append(absl::Span<uint8_t> src) noexcept = 0;

  // Attempts to sync data to underlying storage. If an error was encountered, a
  // non-OK status will be returned.
  //
  // Safe for concurrent use by multiple threads.
  virtual absl::Status Sync() noexcept = 0;
};

}  // namespace io
}  // namespace mybitcask
#endif
