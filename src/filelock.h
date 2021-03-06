#ifndef MYBITCASK_SRC_FILELOCK_H_
#define MYBITCASK_SRC_FILELOCK_H_

#include "absl/status/statusor.h"
#include "ghc/filesystem.hpp"

#include <memory>

namespace mybitcask {
namespace filelock {

class LockFile {
 public:
  // Tries to lock a file and blocks until it is possible to lock.
  virtual absl::Status Lock() = 0;
  // Tries to lock a file but returns as soon as possible if already locked.
  virtual absl::StatusOr<bool> TryLock() = 0;
  // Unlocks the file.
  // If delete_file is true, the file will be deleted when the file is
  // successfully unlocked.
  virtual absl::Status Unlock(bool delete_file = true) = 0;

  virtual ~LockFile() = default;
};

absl::StatusOr<std::unique_ptr<LockFile>> Open(
    const ghc::filesystem::path& filename);

}  // namespace filelock
}  // namespace mybitcask
#endif  // MYBITCASK_SRC_FILELOCK_H_
