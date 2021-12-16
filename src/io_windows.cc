// Windowsrandomaccessreader reference leveldb
#include "mybitcask/internal/io.h"
#include "windows_util.h"

#include <windows.h>
#include <algorithm>

namespace mybitcask {
namespace io {

class WindowsMmapRandomAccessFileReader final : public RandomAccessReader {
 public:
  WindowsMmapRandomAccessFileReader() = delete;
  ~WindowsMmapRandomAccessFileReader() override {
    ::UnmapViewOfFile(mmap_base_);
  }

  absl::StatusOr<std::size_t> ReadAt(
      std::uint64_t offset, absl::Span<std::uint8_t> dst) noexcept override {
    std::size_t offset_size = static_cast<std::size_t>(offset);
    if (offset_size >= length_ - 1) {
      return 0;
    }
    auto actual_size = (std::min)(length_ - offset_size, dst.size());
    std::memcpy(dst.data(), mmap_base_ + offset_size, actual_size);
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
  WindowsRandomAccessFileReader(win::ScopedHandle&& handle)
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
        return absl::InternalError(win::GetWindowsErrorMessage(error_code));
      }
    }
    return actual_size;
  }

 private:
  const win::ScopedHandle handle_;

  friend absl::StatusOr<std::unique_ptr<RandomAccessReader>>
  OpenRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept;
};

absl::StatusOr<std::unique_ptr<RandomAccessReader>> OpenRandomAccessFileReader(
    ghc::filesystem::path&& filename) noexcept {
  DWORD desired_access = GENERIC_READ;
  DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  win::ScopedHandle handle = ::CreateFileA(
      filename.string().c_str(), desired_access, share_mode,
      /*lpSecurityAttributes=*/nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_READONLY,
      /*hTemplateFile=*/nullptr);
  if (!handle.is_valid()) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(win::GetWindowsErrorMessage(error_code));
  }

  return std::unique_ptr<RandomAccessReader>(
      new WindowsRandomAccessFileReader(std::move(handle)));
}

absl::StatusOr<std::unique_ptr<RandomAccessReader>>
OpenMmapRandomAccessFileReader(ghc::filesystem::path&& filename) noexcept {
  DWORD desired_access = GENERIC_READ;
  DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  win::ScopedHandle handle = ::CreateFileA(
      filename.string().c_str(), desired_access, share_mode,
      /*lpSecurityAttributes=*/nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_READONLY,
      /*hTemplateFile=*/nullptr);
  if (!handle.is_valid()) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(win::GetWindowsErrorMessage(error_code));
  }
  win::ScopedHandle mapping =
      ::CreateFileMappingA(handle.get(),
                           /*security attributes=*/nullptr, PAGE_READONLY,
                           /*dwMaximumSizeHigh=*/0,
                           /*dwMaximumSizeLow=*/0,
                           /*lpName=*/nullptr);
  if (!mapping.is_valid()) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(win::GetWindowsErrorMessage(error_code));
  }
  void* mmap_base = ::MapViewOfFile(mapping.get(), FILE_MAP_READ,
                                    /*dwFileOffsetHigh=*/0,
                                    /*dwFileOffsetLow=*/0,
                                    /*dwNumberOfBytesToMap=*/0);
  if (!mmap_base) {
    DWORD error_code = ::GetLastError();
    return absl::InternalError(win::GetWindowsErrorMessage(error_code));
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
