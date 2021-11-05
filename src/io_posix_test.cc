#include "gtest/gtest.h"
#include "io.h"

#include <fstream>

namespace mybitcask {
namespace io {
TEST(IoTest, MmapRandomAccessReader) {
  auto tempfile = test::MakeTempFile("mybitcask_test_", ".tmp");
  ASSERT_TRUE(tempfile.ok());

  char* test_data = "test data.";

  std::ofstream tempfile_stream(tempfile->Filename());
  tempfile_stream << test_data;
  tempfile_stream.close();

  auto reader = OpenRandomAccessReader(tempfile->Filename());
  ASSERT_TRUE(reader.ok());

  char read_data[100];
  (*reader)->ReadAt({read_data, sizeof read_data});
  EXPECT_STREQ(test_data, read_data);
}
}  // namespace io
}  // namespace mybitcask
