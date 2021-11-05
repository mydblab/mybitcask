#include "io.h"


namespace mybitcask {
namespace io {

class WindowsRandomAccessReader : public RandomAccessReader {
 public:
  WindowsRandomAccessReader() = delete;
  ~WindowsRandomAccessReader() override {}

  absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept override {
    return 0;
  }

 private:
  WindowsRandomAccessReader() {}

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessReader(ghc::filesystem::path filename);
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessReader(
    ghc::filesystem::path filename) {
  return nullptr;
}

}  // namespace io
}  // namespace mybitcask
