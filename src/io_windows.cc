// Windowsrandomaccessreader reference leveldb
#include "io.h"

#include <windows.h>

namespace mybitcask {
namespace io {

std::string GetWindowsErrorMessage(DWORD error_code) {
  std::string message;
  char* error_text = nullptr;
  // Use MBCS version of FormatMessage to match return value.
  size_t error_text_size = ::FormatMessageA(
      FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ALLOCATE_BUFFER |
          FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr, error_code, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      reinterpret_cast<char*>(&error_text), 0, nullptr);
  if (!error_text) {
    return message;
  }
  message.assign(error_text, error_text_size);
  ::LocalFree(error_text);
  return message;
}

class WindowsRandomAccessReader : public RandomAccessReader {
 public:
  WindowsRandomAccessReader(HANDLE&& handle)
      : handle_(std::move(handle)){};
  ~WindowsRandomAccessReader() override { ::CloseHandle(handle_); }

  absl::StatusOr<std::size_t> ReadAt(
      std::uint64_t offset, absl::Span<std::uint8_t> dst) noexcept override {
    DWORD actual_size = 0;
    OVERLAPPED overlapped = {0};

    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
    overlapped.Offset = static_cast<DWORD>(offset);
    if (!::ReadFile(handle_, dst.data(), static_cast<DWORD>(dst.size()),
                    &actual_size, &overlapped)) {
      DWORD error_code = ::GetLastError();
      if (error_code != ERROR_HANDLE_EOF) {
        return absl::InternalError(GetWindowsErrorMessage(error_code));
      }
    }
    return actual_size;
  }

 private:
  const HANDLE handle_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessFileReader(const ghc::filesystem::path& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessFileReader(
    const ghc::filesystem::path& filename) noexcept {
  DWORD desired_access = GENERIC_READ;
  DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  HANDLE handle = ::CreateFileA(
      filename.string().c_str(), desired_access, share_mode,
      /*lpSecurityAttributes=*/nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_READONLY,
      /*hTemplateFile=*/nullptr);
  if (handle == nullptr) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(GetWindowsErrorMessage(error_code));
  }

  return std::unique_ptr<RandomAccessReader>(
      new WindowsRandomAccessReader(std::move(handle)));
}

}  // namespace io
}  // namespace mybitcask
