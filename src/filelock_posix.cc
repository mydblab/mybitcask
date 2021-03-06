#include <fcntl.h>
#include <sys/file.h>
#include <cerrno>
#include <cstring>
#include "ghc/filesystem.hpp"

#include "assert.h"
#include "filelock.h"
#include "mybitcask/internal/io.h"

namespace mybitcask {
namespace filelock {

class LockFilePosix final : public LockFile {
 public:
  absl::Status Lock() {
    assertm(!locked_, "Cannot lock if already owning a lock");
    if (flock(fd_, LOCK_EX) >= 0) {
      locked_ = true;
      return absl::OkStatus();
    }
    return absl::InternalError(std::strerror(errno));
  }

  absl::StatusOr<bool> TryLock() {
    assertm(!locked_, "Cannot lock if already owning a lock");
    if (flock(fd_, LOCK_EX | LOCK_NB) >= 0) {
      locked_ = true;
      return true;
    }
    auto en = errno;
    if (en == EWOULDBLOCK || en == EINTR) {
      return false;
    }
    return absl::InternalError(std::strerror(en));
  }

  absl::Status Unlock(bool delete_file = true) {
    assertm(locked_, "Attempted to unlock already locked lockfile");
    if (flock(fd_, LOCK_UN) >= 0) {
      locked_ = false;
      if (delete_file) {
        std::error_code _ec;
        ghc::filesystem::remove(filename_, _ec);
      }
      return absl::OkStatus();
    }
    return absl::InternalError(std::strerror(errno));
  }

  LockFilePosix() = delete;

 private:
  LockFilePosix(bool locked, int fd, const ghc::filesystem::path& filename)
      : locked_(locked), fd_(fd), filename_(filename) {}

  const ghc::filesystem::path& filename_;
  bool locked_;
  int fd_;

  friend absl::StatusOr<std::unique_ptr<LockFile>> Open(
      const ghc::filesystem::path& filename);
};

absl::StatusOr<std::unique_ptr<LockFile>> Open(
    const ghc::filesystem::path& filename) {
  int fd =
      ::open(filename.string().c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (fd < 0) {
    return absl::InternalError(io::kErrOpenFailed);
  }
  return std::unique_ptr<LockFile>(new LockFilePosix(false, fd, filename));
}

}  // namespace filelock
}  // namespace mybitcask
