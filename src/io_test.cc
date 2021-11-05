#include "io.h"
#include "ghc/filesystem.hpp"
#include "gtest/gtest.h"
#include "test_util.h"

#include <fstream>

namespace mybitcask {
namespace io {

TEST(IoTest, FStreamSequentialWriter) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tempfile.ok());

  auto writer = OpenSequentialWriter(tempfile->Filename());
  ASSERT_TRUE(writer.ok());

  const std::string test_data = "test data.";

  auto append_status = (*writer)->Append(
      {reinterpret_cast<const std::uint8_t*>(test_data.c_str()), test_data.size()});
  ASSERT_TRUE(append_status.ok());
  auto sync_status = (*writer)->Sync();
  ASSERT_TRUE(sync_status.ok());
  (*writer).release();

  std::ifstream tempfile_stream(tempfile->Filename());
  std::stringstream read_data;
  read_data << tempfile_stream.rdbuf();
  EXPECT_EQ(read_data.str(), test_data);
}

}  // namespace io
}  // namespace mybitcask
