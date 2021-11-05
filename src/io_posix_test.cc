#include "gtest/gtest.h"
#include "io.h"
#include "test_util.h"

#include <cstring>
#include <fstream>

namespace mybitcask {
namespace io {
TEST(IoTest, MmapRandomAccessReader) {
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
