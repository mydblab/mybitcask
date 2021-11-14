#include "mybitcask/internal/log.h"
#include "mybitcask/internal/io.h"

#include "absl/base/internal/endian.h"
#include "crc32c/crc32c.h"
#include "gtest/gtest.h"
#include "test_util.h"

#include <random>

namespace mybitcask {
namespace log {

absl::StatusOr<std::unique_ptr<log::LogReader>> create_log_reader(
    const ghc::filesystem::path& filename, bool checksum) {
  auto src = io::OpenRandomAccessFileReader(
      std::move(const_cast<ghc::filesystem::path&>(filename)));
  if (!src.ok()) {
    return src.status();
  }
  std::unique_ptr<log::LogReader> log_reader(
      new log::LogReader(*std::move(src), checksum));
  auto status = log_reader->Init();
  if (!status.ok()) {
    return absl::Status(status);
  }
  return log_reader;
}

absl::StatusOr<std::unique_ptr<log::LogWriter>> create_log_writer(
    const ghc::filesystem::path& filename) {
  auto dest = io::OpenSequentialFileWriter(
      std::move(const_cast<ghc::filesystem::path&>(filename)));
  if (!dest.ok()) {
    return dest.status();
  }

  std::unique_ptr<log::LogWriter> log_writer(
      new log::LogWriter(*std::move(dest)));
  auto status = log_writer->Init();
  if (!status.ok()) {
    return absl::Status(status);
  }
  return log_writer;
}

TEST(LogReaderWriterTest, NormalReadWriter) {
  auto tempfile = test::MakeTempFile("mybitcask_test_");
  ASSERT_TRUE(tempfile.ok());
  auto log_reader = create_log_reader(tempfile->filename(), false);
  ASSERT_TRUE(log_reader.ok())
      << "Field to create log reader. error: " << log_reader.status();
  auto log_writer = create_log_writer(tempfile->filename());
  ASSERT_TRUE(log_writer.ok())
      << "Field to create log writer. error: " << log_writer.status();

  struct TestCase {
    std::string key;
    std::string value;
  };

  std::default_random_engine e(std::random_device{}());

  std::vector<TestCase> cases = {
      {"hello", "world"},
      {"lbw", "nb"},
      {"玩游戏一定要", "啸"},
      {test::GenerateRandomString(0xFF),
       test::GenerateRandomString(0xFFFF - 1)},
  };

  absl::Status append_status;
  Position position;
  absl::StatusOr<absl::optional<Entry>> entry_opt;

  for (const auto& c : cases) {
    append_status = (*log_writer)
                        ->Append(test::StrSpan(c.key), test::StrSpan(c.value),
                                 [&](Position pos) { position = pos; });
    ASSERT_TRUE(append_status.ok()) << "append error: " << append_status;
    entry_opt = (*log_reader)->Read(position);
    ASSERT_TRUE(entry_opt.ok()) << "read error: " << entry_opt.status();
    ASSERT_TRUE(entry_opt->has_value());
    EXPECT_EQ((*entry_opt)->key(), test::StrSpan(c.key));
    EXPECT_EQ((*entry_opt)->value(), test::StrSpan(c.value));
  }

  // test read tombstone
  for (const auto& c : cases) {
    append_status =
        (*log_writer)->AppendTombstone(test::StrSpan(c.key), [&](Position pos) {
          position = pos;
        });
    ASSERT_TRUE(append_status.ok()) << "append error: " << append_status;
    entry_opt = (*log_reader)->Read(position);
    ASSERT_TRUE(entry_opt.ok())
        << "read tombstone entry error: " << entry_opt.status();
    ASSERT_FALSE(entry_opt->has_value())
        << "read tombstone entry should return nullopt";
  }
}

TEST(LogReaderTest, ReadEmptyFile) {
  auto tempfile = test::MakeTempFile("mybitcask_test_");
  ASSERT_TRUE(tempfile.ok());
  auto log_reader = create_log_reader(tempfile->filename(), false);
  ASSERT_TRUE(log_reader.ok())
      << "Field to create log reader. error: " << log_reader.status();

  for (std::uint64_t offset : {0, 1, 100}) {
    auto entry_opt = (*log_reader)->Read(Position{offset, 100});
    ASSERT_TRUE(entry_opt.ok()) << "read error: " << entry_opt.status();
    EXPECT_FALSE(entry_opt->has_value())
        << "Reading a log entry from an empty file should return nullopt";
  }
}

class StreamUtil {
 public:
  StreamUtil(std::ofstream* inner) : stream_(inner), offset_(0) {}

  void PushU8(std::uint8_t u8) {
    stream_->put(u8);
    offset_++;
  }

  void PushU16(std::uint16_t u16) {
    std::uint8_t bytes[sizeof(std::uint16_t)]{};
    absl::little_endian::Store16(&bytes, u16);
    Push(bytes);
    offset_ += 2;
  }

  void PushU32(std::uint32_t u32) {
    std::uint8_t bytes[sizeof(std::uint32_t)]{};
    absl::little_endian::Store32(&bytes, u32);
    Push(bytes);
    offset_ += 4;
  }
  void Push(absl::Span<const std::uint8_t> data) {
    stream_->write(reinterpret_cast<const char*>(data.data()), data.size());
  }

  std::size_t offset() const { return offset_; }

 private:
  std::ofstream* stream_;
  std::size_t offset_;
};

TEST(LogWriterTest, ReadWrongEntry) {
  auto tempfile = test::MakeTempFile("mybitcask_test_");
  ASSERT_TRUE(tempfile.ok());
  auto log_reader = create_log_reader(tempfile->filename(), true);
  ASSERT_TRUE(log_reader.ok())
      << "Field to create log reader. error: " << log_reader.status();

  std::ofstream inner(tempfile->filename(), std::ios::binary | std::ios::out);
  StreamUtil w(&inner);

  // test for invalid CRC
  w.PushU32(2718281828);       // invalid CRC32
  w.PushU8(1);                 // key_sz
  w.PushU16(1);                // value_sz
  w.Push(test::StrSpan("a"));  // key
  w.Push(test::StrSpan("b"));  // value
  auto offset = w.offset();
  inner.flush();
  auto entry_opt = (*log_reader)->Read(Position{0, offset});
  EXPECT_FALSE(entry_opt.ok()) << "read an entry with bad crc should fail";
  EXPECT_EQ(entry_opt.status().message(), kErrBadEntry)
      << "read an entry with bad crc should return kErrBadEntry";

  w.PushU32(2718281828);       // invalid CRC32
  w.PushU8(1);                 // key_sz
  w.PushU16(0xFFFF);           // value_sz, kTombstone
  w.Push(test::StrSpan("a"));  // key
  w.Push(test::StrSpan(""));   // value
  inner.flush();
  entry_opt = (*log_reader)->Read(Position{offset, w.offset() - offset});
  offset = w.offset();
  EXPECT_FALSE(entry_opt.ok()) << "read an entry with bad crc should fail";
  EXPECT_EQ(entry_opt.status().message(), kErrBadEntry)
      << "read an entry with bad crc should return kErrBadEntry";

  // Test for incorrect key/value length
  std::uint8_t data[]{
      100,  // incorrect key_sz,
      1,    // value_sz
      'a',  // key
      'b',  // value
  };
  w.PushU32(crc32c::Crc32c(data, 5));
  w.Push(data);
  inner.flush();
  entry_opt = (*log_reader)->Read(Position{offset, w.offset() - offset});
  EXPECT_FALSE(entry_opt.ok())
      << "read an entry with incorrect key/value length should fail";
  EXPECT_EQ(entry_opt.status().message(), kErrBadEntry)
      << "read an entry with incorrect key/value length should return "
         "kErrBadEntry";
}

TEST(LogWriterTest, AppendWithWrongKVLength) {
  auto tempfile = test::MakeTempFile("mybitcask_test_");
  ASSERT_TRUE(tempfile.ok());
  auto log_writer = create_log_writer(tempfile->filename());
  ASSERT_TRUE(log_writer.ok())
      << "Field to create log writer. error: " << log_writer.status();

  // Append entry with empty key
  auto status = (*log_writer)
                    ->Append(test::StrSpan(""), test::StrSpan("not care"),
                             [](Position) {});
  EXPECT_FALSE(status.ok()) << "append entry with an empty key  should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append entry with an empty key should return kErrBadKeyLength";
  status = (*log_writer)->AppendTombstone(test::StrSpan(""), [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append tombstone entry with an empty key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append tombstone entry with an empty key should return "
         "kErrBadKeyLength";

  // Append entry with empty value
  status = (*log_writer)
               ->Append(test::StrSpan("not care"), test::StrSpan(""),
                        [](Position) {});
  EXPECT_FALSE(status.ok()) << "append entry with empty value should fail";
  EXPECT_EQ(status.message(), kErrBadValueLength)
      << "append entry with empty value should return kErrBadValueLength";

  // Append entry with an oversized key
  status =
      (*log_writer)
          ->Append(test::StrSpan(test::GenerateRandomString(0xFF, 0xFF + 100)),
                   test::StrSpan("not care"), [](Position) {});
  EXPECT_FALSE(status.ok()) << "append entry with an oversized key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append entry with an oversized key should return kErrBadKeyLength";

  status = (*log_writer)
               ->AppendTombstone(
                   test::StrSpan(test::GenerateRandomString(0xFF, 0xFF + 100)),
                   [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append tombstone entry with an oversized key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append tombstone entry with an oversized key should return "
         "kErrBadKeyLength";

  // Append entry with an oversized value
  status =
      (*log_writer)
          ->Append(
              test::StrSpan("not care"),
              test::StrSpan(test::GenerateRandomString(0xFFFF, 0xFFFF + 200)),
              [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append entry with an oversized value should fail";
  EXPECT_EQ(status.message(), kErrBadValueLength)
      << "append entry with an oversized value should return "
         "kErrBadValueLength";
}

}  // namespace log
}  // namespace mybitcask
