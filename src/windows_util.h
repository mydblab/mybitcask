#ifndef MYBITCASK_SRC_WINDOWS_UTIL_H_
#define MYBITCASK_SRC_WINDOWS_UTIL_H_

#include <windows.h>

namespace mybitcask {
namespace win {

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
}  // namespace win
}  // namespace mybitcask
#endif  // MYBITCASK_SRC_WINDOWS_UTIL_H_
