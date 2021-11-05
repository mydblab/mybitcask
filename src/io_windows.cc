#include "io.h"

namespace mybitcask {
namespace io {

class WindowsRandomAccessReader : public RandomAccessReader {
 public:
  WindowsRandomAccessReader() = default;
  ~WindowsRandomAccessReader() override {}

  absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept override {
    return 0;
  }

 private:
  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessReader(const ghc::filesystem::path& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessReader(
    const ghc::filesystem::path& filename) noexcept {
  return std::unique_ptr<RandomAccessReader>(new WindowsRandomAccessReader());
}

}  // namespace io
}  // namespace mybitcask
