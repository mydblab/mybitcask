#include "assert.h"
#include "filelock.h"
#include "windows_util.h"

#include "ghc/filesystem.hpp"

namespace mybitcask {
namespace filelock {
class LockFileWindows final : public LockFile {
 public:
  absl::Status Lock() {
    assertm(!locked_, "Cannot lock if already owning a lock");
    OVERLAPPED ov = {0};
    if (!::LockFileEx(sh_.get(), LOCKFILE_EXCLUSIVE_LOCK, 0, MAXDWORD, MAXDWORD,
                      &ov)) {
      DWORD error_code = ::GetLastError();
      return absl::InternalError(win::GetWindowsErrorMessage(error_code));
    }
    locked_ = true;
    return absl::OkStatus();
  }

  absl::StatusOr<bool> TryLock() {
    assertm(!locked_, "Cannot lock if already owning a lock");

    OVERLAPPED ov = {0};
    if (!::LockFileEx(sh_.get(),
                      LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0,
                      MAXDWORD, MAXDWORD, &ov)) {
      DWORD error_code = ::GetLastError();
      if (error_code == ERROR_LOCK_VIOLATION) {
        return false;
      }
      return absl::InternalError(win::GetWindowsErrorMessage(error_code));
    }
    locked_ = true;
    return true;
  }

  absl::Status Unlock(bool delete_file = true) {
    assertm(locked_, "Attempted to unlock already locked lockfile");
    OVERLAPPED ov = {0};
    if (!::UnlockFileEx(sh_.get(), 0, MAXDWORD, MAXDWORD, &ov)) {
      DWORD error_code = ::GetLastError();
      return absl::InternalError(win::GetWindowsErrorMessage(error_code));
    }
    locked_ = false;
    if (delete_file && sh_.Close()) {
      std::error_code _ec;
      // The file may be held by another lock, so the current delete operation may fail
      ghc::filesystem::remove(filename_, _ec);
    }
    return absl::OkStatus();
  }

 private:
  LockFileWindows(bool locked, win::ScopedHandle&& sh,
                  const ghc::filesystem::path& filename)
      : locked_(locked), sh_(std::move(sh)), filename_(filename) {}

  const ghc::filesystem::path& filename_;
  bool locked_;
  win::ScopedHandle sh_;

  friend absl::StatusOr<std::unique_ptr<LockFile>> Open(
      const ghc::filesystem::path& filename);
};

absl::StatusOr<std::unique_ptr<LockFile>> Open(
    const ghc::filesystem::path& filename) {
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

  return std::unique_ptr<LockFile>(
      new LockFileWindows(false, std::move(handle), filename));
}

}  // namespace filelock
}  // namespace mybitcask
