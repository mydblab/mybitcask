#include "io.h"

#include <windows.h>

namespace mybitcask {
namespace io {

class WindowsRandomAccessReader : public RandomAccessReader {
 public:
  WindowsRandomAccessReader(const HANDLE&& handle)
      : handle_(std::move(handle)){};
  ~WindowsRandomAccessReader() override { ::CloseHandle(handle_); }

  absl::StatusOr<std::size_t> ReadAt(
      std::uint64_t offset, absl::Span<std::uint8_t> dst) const noexcept override {
    DWORD actual_size = 0;
    OVERLAPPED overlapped = {0};

    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
    overlapped.Offset = static_cast<DWORD>(offset);
    if (!::ReadFile(handle_, dst.data(), static_cast<DWORD>(dst.size()),
                    &actual_size, &overlapped)) {
      DWORD error_code = ::GetLastError();
      if (error_code != ERROR_HANDLE_EOF) {
        *result = Slice(scratch, 0);
        return absl::InternalError(GetWindowsErrorMessage(error_code));
      }
    }
    return actual_size;
  }

 private:
  const HANDLE handle_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessReader(const ghc::filesystem::path& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessReader(
    const ghc::filesystem::path& filename) noexcept {
  DWORD desired_access = GENERIC_READ;
  DWORD share_mode = FILE_SHARE_READ;
  HANDLE handle = ::CreateFileA(filename.c_str(), desired_access, share_mode,
                                /*lpSecurityAttributes=*/nullptr, OPEN_EXISTING,
                                FILE_ATTRIBUTE_READONLY,
                                /*hTemplateFile=*/nullptr);
  if (!handle.is_valid()) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(GetWindowsErrorMessage(error_code));
  }
  return std::unique_ptr<RandomAccessReader>(
      new WindowsRandomAccessReader(handle));
}

}  // namespace io
}  // namespace mybitcask
