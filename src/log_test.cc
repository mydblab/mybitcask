#include "mybitcask/internal/log.h"
#include "mybitcask/internal/io.h"
#include "mybitcask/mybitcask.h"

#include "absl/base/internal/endian.h"
#include "absl/types/optional.h"
#include "crc32c/crc32c.h"
#include "ghc/filesystem.hpp"
#include "gtest/gtest.h"
#include "test_util.h"

#include <memory>
#include <random>
#include "log.cc"

namespace mybitcask {
namespace log {

absl::StatusOr<absl::optional<Entry>> log_read(Reader* log_reader,
                                               const Position& pos,
                                               const std::string& key) {
  auto entry = log_reader->Read(pos, key.length());
  std::vector<std::uint8_t> value;
  value.resize(pos.value_len);
  auto exist = log_reader->Read(pos, key.length(), value.data());
  EXPECT_EQ(entry.ok(), exist.ok());
  if (entry.ok()) {
    EXPECT_EQ(entry->has_value(), *exist);
    if (entry->has_value()) {
      EXPECT_EQ((*entry)->value(), absl::MakeSpan(value));
    }
  }
  return entry;
}

TEST(LogReaderWriterTest, NormalReadWriter) {
  auto tmpdir = test::MakeTempDir("mybitcask_log_");
  ASSERT_TRUE(tmpdir.ok());
  store::Store store(store::LogFiles(tmpdir->path()), 128 * 1024 * 1024);
  Reader log_reader(&store, false);
  Writer log_writer(&store);

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
  Position position{};
  absl::StatusOr<absl::optional<Entry>> entry_opt;

  for (const auto& c : cases) {
    append_status =
        log_writer.Append(test::StrSpan(c.key), test::StrSpan(c.value),
                          [&](Position pos) { position = pos; });
    ASSERT_TRUE(append_status.ok()) << "append error: " << append_status;
    entry_opt = log_read(&log_reader, position, c.key);
    ASSERT_TRUE(entry_opt.ok()) << "read error: " << entry_opt.status();
    ASSERT_TRUE(entry_opt->has_value());
    EXPECT_EQ((*entry_opt)->key(), test::StrSpan(c.key));
    EXPECT_EQ((*entry_opt)->value(), test::StrSpan(c.value));
  }

  // test read tombstone
  for (const auto& c : cases) {
    append_status = log_writer.AppendTombstone(
        test::StrSpan(c.key), [&](Position pos) { position = pos; });
    ASSERT_TRUE(append_status.ok()) << "append error: " << append_status;
    entry_opt = log_read(&log_reader, position, c.key);
    ASSERT_TRUE(entry_opt.ok())
        << "read tombstone entry error: " << entry_opt.status();
    ASSERT_FALSE(entry_opt->has_value())
        << "read tombstone entry should return nullopt";
  }
}

class StoreUtil {
 public:
  StoreUtil(store::Store* inner) : store_(inner), offset_(0) {}

  void PushU8(std::uint8_t u8) {
    Push({&u8, 1});
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
    auto _ = store_->Append(data, [](store::Position) {});
  }

  std::uint32_t offset() const { return offset_; }

 private:
  store::Store* store_;
  std::uint32_t offset_;
};

TEST(LogWriterTest, ReadWrongEntry) {
  auto tmpdir = test::MakeTempDir("mybitcask_log_");
  ASSERT_TRUE(tmpdir.ok());
  store::Store store(store::LogFiles(tmpdir->path()), 128 * 1024 * 1024);
  Reader log_reader(&store, true);

  StoreUtil w(&store);

  // test for invalid CRC
  w.PushU32(2718281828);       // invalid CRC32
  w.PushU8(1);                 // key_sz
  w.PushU16(1);                // value_sz
  w.Push(test::StrSpan("a"));  // key
  w.Push(test::StrSpan("b"));  // value
  auto offset = w.offset();
  auto status = store.Sync();
  ASSERT_TRUE(status.ok());
  auto entry_opt = log_read(&log_reader, Position{1, kHeaderLen + 1, 1}, "a");
  EXPECT_FALSE(entry_opt.ok()) << "read an entry with bad crc should fail";
  EXPECT_EQ(entry_opt.status().message(), kErrBadEntry)
      << "read an entry with bad crc should return kErrBadEntry";

  w.PushU32(2718281828);       // invalid CRC32
  w.PushU8(1);                 // key_sz
  w.PushU16(0xFFFF);           // value_sz, kTombstone
  w.Push(test::StrSpan("a"));  // key
  w.Push(test::StrSpan(""));   // value
  status = store.Sync();
  ASSERT_TRUE(status.ok());
  entry_opt =
      log_read(&log_reader,
               Position{1, kHeaderLen + offset,
                        static_cast<std::uint16_t>(w.offset() - offset)},
               "a");
  offset = w.offset();
  EXPECT_FALSE(entry_opt.ok()) << "read an entry with bad crc should fail";
  EXPECT_EQ(entry_opt.status().message(), kErrBadEntry)
      << "read an entry with bad crc should return kErrBadEntry";
}

TEST(LogWriterTest, AppendWithWrongKVLength) {
  auto tmpdir = test::MakeTempDir("mybitcask_log_");
  ASSERT_TRUE(tmpdir.ok());
  store::Store store(store::LogFiles(tmpdir->path()), 128 * 1024 * 1024);
  Writer log_writer(&store);

  // Append entry with empty key
  auto status = log_writer.Append(test::StrSpan(""), test::StrSpan("not care"),
                                  [](Position) {});
  EXPECT_FALSE(status.ok()) << "append entry with an empty key  should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append entry with an empty key should return kErrBadKeyLength";
  status = log_writer.AppendTombstone(test::StrSpan(""), [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append tombstone entry with an empty key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append tombstone entry with an empty key should return "
         "kErrBadKeyLength";

  // Append entry with empty value
  status = log_writer.Append(test::StrSpan("not care"), test::StrSpan(""),
                             [](Position) {});
  EXPECT_FALSE(status.ok()) << "append entry with empty value should fail";
  EXPECT_EQ(status.message(), kErrBadValueLength)
      << "append entry with empty value should return kErrBadValueLength";

  // Append entry with an oversized key
  status = log_writer.Append(
      test::StrSpan(test::GenerateRandomString(0xFF, 0xFF + 100)),
      test::StrSpan("not care"), [](Position) {});
  EXPECT_FALSE(status.ok()) << "append entry with an oversized key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append entry with an oversized key should return kErrBadKeyLength";

  status = log_writer.AppendTombstone(
      test::StrSpan(test::GenerateRandomString(0xFF, 0xFF + 100)),
      [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append tombstone entry with an oversized key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append tombstone entry with an oversized key should return "
         "kErrBadKeyLength";

  // Append entry with an oversized value
  status = log_writer.Append(
      test::StrSpan("not care"),
      test::StrSpan(test::GenerateRandomString(0xFFFF, 0xFFFF + 200)),
      [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append entry with an oversized value should fail";
  EXPECT_EQ(status.message(), kErrBadValueLength)
      << "append entry with an oversized value should return "
         "kErrBadValueLength";
}

TEST(KeyIterTest, Fold) { auto tmpdir = test::MakeTempDir("mybitcask_log_"); }

}  // namespace log
}  // namespace mybitcask
