#include "filelock.h"
#include "gtest/gtest.h"
#include "test_util.h"

namespace mybitcask {
namespace filelock {

TEST(LockFile, Lock) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".lock");
  ASSERT_TRUE(tempfile.ok());
  auto file = Open(tempfile->path());
  ASSERT_TRUE(file.ok());
  ASSERT_TRUE((*file)->Lock().ok());

  auto file1 = Open(tempfile->path());
  ASSERT_TRUE(file1.ok());
  auto lock_ok = (*file1)->TryLock();
  ASSERT_TRUE(lock_ok.ok());
  ASSERT_FALSE(*lock_ok);
  ASSERT_TRUE((*file)->Unlock().ok());
  lock_ok = (*file1)->TryLock();
  ASSERT_TRUE(lock_ok.ok());
  ASSERT_TRUE(*lock_ok);
  ASSERT_TRUE((*file1)->Unlock().ok());
}

}  // namespace filelock
}  // namespace mybitcask
