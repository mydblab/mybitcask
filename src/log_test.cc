#include "log.h"
#include "gtest/gtest.h"
#include "io.h"
#include "test_util.h"

namespace mybitcask {
namespace log {

TEST(LogReaderTest, NormalRead) {
  auto tempfile = test::MakeTempFile("mybitcask_test_");
  ASSERT_TRUE(tempfile.ok());
  auto temp_src = io::OpenRandomAccessReader((*tempfile).Filename());
  EXPECT_TRUE(temp_src.ok());
  auto temp_dest = io::OpenSequentialWriter((*tempfile).Filename());
  EXPECT_TRUE(temp_dest.ok());

  log::LogReader log_reader(*std::move(temp_src));
  log::LogWriter log_writer(*std::move(temp_dest));

  log_writer.Append({"hello", 5}, {"world", 5});
  log_reader.Read(0);

  // log_reader.Read();
}

}  // namespace log
}  // namespace mybitcask
