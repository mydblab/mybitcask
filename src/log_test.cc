#include "log.h"
#include "io.h"
#include "test_util.h"

#include <gtest/gtest.h>

namespace mybitcask {
namespace log {


TEST(LogReaderTest, Read) {

  auto status_temp_src = io::OpenRandomAccessReader(test::TempFilename());
  EXPECT_TRUE(status_temp_src.ok());
  log::LogReader log_reader(std::move(status_temp_src).value());
  log_reader.Read();
}

}  // namespace log
}  // namespace mybitcask
