#include "log.h"
#include "gtest/gtest.h"
#include "io.h"
#include "test_util.h"

namespace mybitcask {
namespace log {

absl::StatusOr<std::unique_ptr<log::LogReader>> create_log_reader(
    const ghc::filesystem::path& filename) {
  auto src = io::OpenRandomAccessFileReader(filename);
  if (!src.ok()) {
    return src.status();
  }
  std::unique_ptr<log::LogReader> log_reader(
      new log::LogReader(*std::move(src)));
  log_reader->Init();

  return log_reader;
}

absl::StatusOr<std::unique_ptr<log::LogWriter>> create_log_writer(
    const ghc::filesystem::path& filename) {
  auto dest = io::OpenSequentialFileWriter(filename);
  if (!dest.ok()) {
    return dest.status();
  }

  std::unique_ptr<log::LogWriter> log_writer(
      new log::LogWriter(*std::move(dest)));
  log_writer->Init();

  return log_writer;
}

TEST(LogReaderTest, NormalRead) {
  auto tempfile = test::MakeTempFile("mybitcask_test_");
  ASSERT_TRUE(tempfile.ok());
  auto log_reader = create_log_reader(tempfile->filename());
  ASSERT_TRUE(log_reader.ok())
      << "Field to create log reader. error: " << log_reader.status();
  auto log_writer = create_log_writer(tempfile->filename());
  ASSERT_TRUE(log_writer.ok())
      << "Field to create log writer. error: " << log_writer.status();

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
    offset =
        (*log_writer)->Append(test::StrSpan(c.key), test::StrSpan(c.value));
    ASSERT_TRUE(offset.ok()) << "append error: " << offset.status();
    entry_opt = (*log_reader)->Read(0);
    ASSERT_TRUE(entry_opt.ok()) << "read error: " << entry_opt.status();
    ASSERT_TRUE(entry_opt->has_value());
    EXPECT_EQ((*entry_opt)->key(), test::StrSpan(c.key));
    EXPECT_EQ((*entry_opt)->value(), test::StrSpan(c.value));
  }
}

TEST(LogReaderTest, Tombstone) {}

TEST(LogReaderTest, ReadEmptyFile) {
  auto tempfile = test::MakeTempFile("mybitcask_test_");
  ASSERT_TRUE(tempfile.ok());
  auto log_reader = create_log_reader(tempfile->filename());
  ASSERT_TRUE(log_reader.ok())
      << "Field to create log reader. error: " << log_reader.status();

  for (auto offset : {0, 1, 100}) {
    auto entry_opt = (*log_reader)->Read(offset);
    ASSERT_TRUE(entry_opt.ok()) << "read error: " << entry_opt.status();
    EXPECT_FALSE(entry_opt->has_value())
        << "Reading a log entry from an empty file should return nullopt";
  }
}

}  // namespace log
}  // namespace mybitcask
