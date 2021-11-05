

#include "io.h"
#include "ghc/filesystem.hpp"
#include "test_util.h"

#include <gtest/gtest.h>
#include <cstdio>
#include <iostream>

namespace mybitcask {
namespace io {

TEST(IoTest, FStreamSequentialWriter) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tempfile.ok());

  auto writer = OpenSequentialWriter(tempfile->Filename());
  ASSERT_TRUE(writer.ok());
  std::string_view testdata = "test data.";
  auto append_status =
      (*writer)->Append({reinterpret_cast<const std::uint8_t*>(testdata.data()),
                         sizeof testdata});
  ASSERT_TRUE(append_status.ok());
  auto sync_status = (*writer)->Sync();
  ASSERT_TRUE(sync_status.ok());
  (*writer).release();
}

}  // namespace io
}  // namespace mybitcask
