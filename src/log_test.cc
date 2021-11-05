#include "log.h"
#include "io.h"
#include "test_util.h"

#include <gtest/gtest.h>

namespace mybitcask {
namespace log {

TEST(LogReaderTest, Read) {
  auto temp_filename = test::TempFilename("mybitcask_test");
  std::cout << *temp_filename << std::endl;
  ASSERT_TRUE(temp_filename.ok());
  auto temp_src = io::OpenRandomAccessReader(*temp_filename);
  EXPECT_TRUE(temp_src.ok());
  // log::LogReader log_reader(*std::move(temp_src));
  // log_reader.Read();
}

}  // namespace log
}  // namespace mybitcask
