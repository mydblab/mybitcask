// Windowsrandomaccessreader reference leveldb
#include "mybitcask/internal/io.h"

#include <windows.h>
#include <algorithm>

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

class ScopedHandle {
 public:
  ScopedHandle(HANDLE handle) : handle_(handle) {}
  ScopedHandle(const ScopedHandle&) = delete;
  ScopedHandle(ScopedHandle&& other) noexcept : handle_(other.Release()) {}
  ~ScopedHandle() { Close(); }

  ScopedHandle& operator=(const ScopedHandle&) = delete;

  ScopedHandle& operator=(ScopedHandle&& rhs) noexcept {
    if (this != &rhs) handle_ = rhs.Release();
    return *this;
  }

  bool Close() {
    if (!is_valid()) {
      return true;
    }
    HANDLE h = handle_;
    handle_ = INVALID_HANDLE_VALUE;
    return ::CloseHandle(h);
  }

  bool is_valid() const {
    return handle_ != INVALID_HANDLE_VALUE && handle_ != nullptr;
  }

  HANDLE get() const { return handle_; }

  HANDLE Release() {
    HANDLE h = handle_;
    handle_ = INVALID_HANDLE_VALUE;
    return h;
  }

 private:
  HANDLE handle_;
};

class WindowsMmapRandomAccessFileReader final : public RandomAccessReader {
 public:
  WindowsMmapRandomAccessFileReader() = delete;
  ~WindowsMmapRandomAccessFileReader() override {
    ::UnmapViewOfFile(mmap_base_);
  }

  absl::StatusOr<std::size_t> ReadAt(
      uint64_t offset, absl::Span<std::uint8_t> dst) noexcept override {
    if (offset >= length_ - 1) {
      return absl::OutOfRangeError("offset is too large");
    }
    auto actual_size =
        (std::min)(static_cast<std::size_t>(length_ - offset), dst.size());
    std::memcpy(dst.data(), mmap_base_ + offset, actual_size);
    return actual_size;
  }

 private:
  WindowsMmapRandomAccessFileReader(std::uint8_t* mmap_base, std::size_t length)
      : mmap_base_(mmap_base), length_(length) {}

  std::uint8_t* const mmap_base_;
  const std::size_t length_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenMmapRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept;
};

class WindowsRandomAccessFileReader final : public RandomAccessReader {
 public:
  WindowsRandomAccessFileReader(ScopedHandle&& handle)
      : handle_(std::move(handle)){};
  ~WindowsRandomAccessFileReader() override = default;

  absl::StatusOr<std::size_t> ReadAt(
      std::uint64_t offset, absl::Span<std::uint8_t> dst) noexcept override {
    DWORD actual_size = 0;
    OVERLAPPED overlapped = {0};

    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
    overlapped.Offset = static_cast<DWORD>(offset);
    if (!::ReadFile(handle_.get(), dst.data(), static_cast<DWORD>(dst.size()),
                    &actual_size, &overlapped)) {
      DWORD error_code = ::GetLastError();
      if (error_code != ERROR_HANDLE_EOF) {
        return absl::InternalError(GetWindowsErrorMessage(error_code));
      }
    }
    return actual_size;
  }

  void Obsolete() noexcept {}

 private:
  const ScopedHandle handle_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessFileReader(
    ghc::filesystem::path&& filename) noexcept {
  DWORD desired_access = GENERIC_READ;
  DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  ScopedHandle handle = ::CreateFileA(
      filename.string().c_str(), desired_access, share_mode,
      /*lpSecurityAttributes=*/nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_READONLY,
      /*hTemplateFile=*/nullptr);
  if (!handle.is_valid()) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(GetWindowsErrorMessage(error_code));
  }

  return std::unique_ptr<RandomAccessReader>(
      new WindowsRandomAccessFileReader(std::move(handle)));
}

absl::StatusOr<std::unique_ptr<RandomAccessReader>>
OpenMmapRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept {
  DWORD desired_access = GENERIC_READ;
  DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  ScopedHandle handle = ::CreateFileA(
      filename.string().c_str(), desired_access, share_mode,
      /*lpSecurityAttributes=*/nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_READONLY,
      /*hTemplateFile=*/nullptr);
  if (!handle.is_valid()) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(GetWindowsErrorMessage(error_code));
  }
  ScopedHandle mapping =
      ::CreateFileMappingA(handle.get(),
                           /*security attributes=*/nullptr, PAGE_READONLY,
                           /*dwMaximumSizeHigh=*/0,
                           /*dwMaximumSizeLow=*/0,
                           /*lpName=*/nullptr);
  if (!mapping.is_valid()) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(GetWindowsErrorMessage(error_code));
  }
  void* mmap_base = ::MapViewOfFile(mapping.get(), FILE_MAP_READ,
                                    /*dwFileOffsetHigh=*/0,
                                    /*dwFileOffsetLow=*/0,
                                    /*dwNumberOfBytesToMap=*/0);
  if (!mmap_base) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(GetWindowsErrorMessage(error_code));
  }
  auto file_size = GetFileSize(filename);

  if (!file_size.ok()) {
    return absl::Status(file_size.status());
  }
  return std::unique_ptr<RandomAccessReader>(
      new WindowsMmapRandomAccessFileReader(
          reinterpret_cast<std::uint8_t*>(mmap_base), *file_size));
}

}  // namespace io
}  // namespace mybitcask
