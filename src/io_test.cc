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

  const std::string test_data = "test data.";

  {
    auto filename = tempfile->filename();
    auto writer = OpenSequentialFileWriter(std::move(filename));
    ASSERT_TRUE(writer.ok());
    auto file_size = (*writer)->Size();
    ASSERT_TRUE(file_size.ok());
    ASSERT_EQ(*file_size, 0);
    auto append_status = (*writer)->Append(
        {reinterpret_cast<const std::uint8_t*>(test_data.c_str()),
         test_data.size()});
    ASSERT_TRUE(append_status.ok());
    auto sync_status = (*writer)->Sync();

    file_size = (*writer)->Size();
    ASSERT_TRUE(file_size.ok());
    ASSERT_EQ(*file_size, test_data.size());

    ASSERT_TRUE(sync_status.ok());
    // The writer outside the current scope will be destructed
  }

  std::ifstream tempfile_stream(tempfile->filename());
  std::stringstream read_data;
  read_data << tempfile_stream.rdbuf();
  tempfile_stream.close();

  EXPECT_EQ(read_data.str(), test_data);
}

TEST(IoTest, RandomAccessFileReader) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tempfile.ok());

  const std::string test_data = "test data.";

  std::ofstream tempfile_stream(tempfile->filename());
  tempfile_stream << test_data;
  tempfile_stream.close();

  auto filename = tempfile->filename();
  auto reader = OpenRandomAccessFileReader(std::move(filename));
  ASSERT_TRUE(reader.ok());

  char* read_data = new char[test_data.size() + 1];
  auto actual_size = (*reader)->ReadAt(
      0, {reinterpret_cast<std::uint8_t*>(read_data), test_data.size()});
  ASSERT_TRUE(actual_size.ok());
  EXPECT_EQ(*actual_size, test_data.size());
  read_data[*actual_size] = '\0';
  EXPECT_EQ(test_data, read_data);
}

TEST(IoTest, MmapRandomAccessFileReader) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tempfile.ok());

  const std::string test_data = "test data.";

  std::ofstream tempfile_stream(tempfile->filename());
  tempfile_stream << test_data;
  tempfile_stream.close();

  auto filename = tempfile->filename();
  auto reader = OpenMmapRandomAccessFileReader(std::move(filename));
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
