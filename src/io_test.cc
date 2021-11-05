#include "io.h"
#include "ghc/filesystem.hpp"
#include "gtest/gtest.h"
#include "test_util.h"

#include <fstream>

namespace mybitcask {
namespace io {

TEST(IoTest, SequentialWriter) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tempfile.ok());
  auto writer = OpenSequentialWriter(tempfile->Filename());
  ASSERT_TRUE(writer.ok());
  const std::string test_data = "test data.";

  auto append_status = (*writer)->Append(
      {reinterpret_cast<const std::uint8_t*>(test_data.c_str()),
       test_data.size()});
  ASSERT_TRUE(append_status.ok());
  auto sync_status = (*writer)->Sync();
  ASSERT_TRUE(sync_status.ok());
  (*writer).release();

  std::ifstream tempfile_stream(tempfile->Filename());
  std::stringstream read_data;
  read_data << tempfile_stream.rdbuf();
  EXPECT_EQ(read_data.str(), test_data);
}

TEST(IoTest, RandomAccessReader) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tempfile.ok());

  const std::string test_data = "test data.";

  std::ofstream tempfile_stream(tempfile->Filename());
  tempfile_stream << test_data;
  tempfile_stream.close();

  auto reader = OpenRandomAccessReader(tempfile->Filename());
  ASSERT_TRUE(reader.ok());

  char* read_data = new char[test_data.size() + 1];

  auto actual_size = (*reader)->ReadAt(
      0, {reinterpret_cast<std::uint8_t*>(read_data), test_data.size()});
  ASSERT_TRUE(actual_size.ok());
  EXPECT_EQ(*actual_size, test_data.size());
  read_data[*actual_size] = '\0';
  EXPECT_EQ(test_data, read_data);
}

}  // namespace io
}  // namespace mybitcask
