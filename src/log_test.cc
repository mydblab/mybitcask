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
  ASSERT_TRUE(temp_src.ok());
  auto temp_dest = io::OpenSequentialWriter((*tempfile).Filename());
  ASSERT_TRUE(temp_dest.ok());

  log::LogReader log_reader(*std::move(temp_src));
  log_reader.Init();
  log::LogWriter log_writer(*std::move(temp_dest));
  log_writer.Init();

  struct TestCase {
    char* key;
    char* value;
  };

  std::vector<TestCase> cases = {
      {"hello", "value"},
      {"lbw", "nb"},
  };

  absl::StatusOr<std::uint64_t> offset;
  absl::StatusOr<absl::optional<Entry>> entry_opt;

  for (const auto& c : cases) {
    offset = log_writer.Append(test::StrSpan(c.key), test::StrSpan(c.value));
    ASSERT_TRUE(offset.ok()) << "append error: " << offset.status();
    entry_opt = log_reader.Read(0);
    ASSERT_TRUE(entry_opt.ok()) << "read error: " << entry_opt.status();
    ASSERT_TRUE((*entry_opt).has_value());
    ASSERT_EQ((**entry_opt).key(), test::StrSpan(c.key));
    ASSERT_EQ((**entry_opt).value(), test::StrSpan(c.value));
  }
}

}  // namespace log
}  // namespace mybitcask
