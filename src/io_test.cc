

#include "io.h"
#include "ghc/filesystem.hpp"
#include "test_util.h"

#include <gtest/gtest.h>
#include <cstdio>
#include <iostream>

namespace mybitcask {
namespace io {

TEST(IoTest, FStreamSequentialWriter) {
  auto tmpfilename = test::TempFilename("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tmpfilename.ok());

  auto writer = OpenSequentialWriter(*tmpfilename);
  ASSERT_TRUE(writer.ok());
  std::string_view testdata = "test data.";
  auto append_status =
      (*writer)->Append({reinterpret_cast<const std::uint8_t*>(testdata.data()),
                         sizeof testdata});
  ASSERT_TRUE(append_status.ok());
  auto sync_status = (*writer)->Sync();
  ASSERT_TRUE(sync_status.ok());
  (*writer).release();

  std::error_code ec;
  ghc::filesystem::remove(*tmpfilename, ec);
  ASSERT_TRUE(ec);
}

}  // namespace io
}  // namespace mybitcask
