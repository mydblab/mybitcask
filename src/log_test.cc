#include "mybitcask/internal/log.h"
#include "mybitcask/internal/io.h"
#include "mybitcask/internal/store.h"
#include "mybitcask/mybitcask.h"
#include "store_dbfiles.h"

#include "absl/base/internal/endian.h"
#include "absl/types/optional.h"
#include "crc32c/crc32c.h"
#include "ghc/filesystem.hpp"
#include "gtest/gtest.h"
#include "test_util.h"

#include <memory>
#include <random>

namespace mybitcask {
namespace log {

absl::StatusOr<absl::optional<Entry>> log_read(Reader* log_reader,
                                               const Position& pos,
                                               const std::string& key) {
  auto entry = log_reader->Read(pos, static_cast<std::uint8_t>(key.length()));
  std::vector<std::uint8_t> value;
  value.resize(pos.value_len);
  auto exist = log_reader->Read(pos, static_cast<std::uint8_t>(key.length()),
                                value.data());
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
  store::Store store(tmpdir->path(),
                     store::DBFiles(tmpdir->path()).latest_file_id(),
                     128 * 1024 * 1024);
  Reader log_reader(&store, false);
  Writer log_writer(&store);

  struct TestCase {
    std::string key;
    std::string value;
  };

  std::vector<TestCase> cases = {
      {"hello", "world"},
      {"lbw", "nb"},
      {"玩游戏一定要", "啸"},
      {test::RandomString(0xFF), test::RandomString(0xFFFF - 1)},
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
  store::Store store(tmpdir->path(),
                     store::DBFiles(tmpdir->path()).latest_file_id(),
                     128 * 1024 * 1024);
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
  auto entry_opt =
      log_read(&log_reader, Position{1, log_internal::kHeaderLen + 1, 1}, "a");
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
               Position{1, log_internal::kHeaderLen + offset,
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
  store::Store store(tmpdir->path(),
                     store::DBFiles(tmpdir->path()).latest_file_id(),
                     128 * 1024 * 1024);
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
  status =
      log_writer.Append(test::StrSpan(test::RandomString(0x100, 0x100 + 100)),
                        test::StrSpan("not care"), [](Position) {});
  EXPECT_FALSE(status.ok()) << "append entry with an oversized key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append entry with an oversized key should return kErrBadKeyLength";

  status = log_writer.AppendTombstone(
      test::StrSpan(test::RandomString(0x100, 0x100 + 100)), [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append tombstone entry with an oversized key should fail";
  EXPECT_EQ(status.message(), kErrBadKeyLength)
      << "append tombstone entry with an oversized key should return "
         "kErrBadKeyLength";

  // Append entry with an oversized value
  status = log_writer.Append(
      test::StrSpan("not care"),
      test::StrSpan(test::RandomString(0xFFFF, 0xFFFF + 200)), [](Position) {});
  EXPECT_FALSE(status.ok())
      << "append entry with an oversized value should fail";
  EXPECT_EQ(status.message(), kErrBadValueLength)
      << "append entry with an oversized value should return "
         "kErrBadValueLength";
}

TEST(KeyIterTest, Fold) {
  auto tmpdir = test::MakeTempDir("mybitcask_log_");
  ASSERT_TRUE(tmpdir.ok());
  store::Store store(tmpdir->path(),
                     store::DBFiles(tmpdir->path()).latest_file_id(), 512);
  Writer log_writer(&store);
  Reader log_reader(&store, false);

  std::vector<test::TestEntry> entries;
  int entries_size = 100;
  for (int i = 0; i < entries_size; i++) {
    auto entry = test::RandomEntry();
    entries.push_back(entry);
    ASSERT_TRUE(test::AppendTestEntry(&log_writer, entry).ok());
  }

  store::DBFiles log_files(tmpdir->path());
  store::file_id_t latest_file_id;

  if (!log_files.active_log_files().empty()) {
    latest_file_id = log_files.active_log_files().back();
  } else {
    latest_file_id = log_files.older_log_files().back();
  }

  struct TestKey {
    std::string key;
    bool is_tombstone;
  };
  struct Void {};
  std::vector<TestKey> fold_keys;
  for (store::file_id_t i = 1; i <= latest_file_id; i++) {
    auto key_iter = log_reader.key_iter(i);
    auto status = key_iter.Fold<
        Void, std::string>(Void(), [&](Void&&, Key<std::string>&& key) {
      fold_keys.push_back(TestKey{key.key_data, !key.value_pos.has_value()});
      return Void();
    });
    ASSERT_TRUE(status.ok())
        << "Failed to fold keys; err: " << status.status().message();
  }

  ASSERT_EQ(entries.size(), fold_keys.size());
  for (int i = 0; i < fold_keys.size(); i++) {
    EXPECT_EQ(entries[i].key, fold_keys[i].key);
    EXPECT_EQ(!entries[i].value.has_value(), fold_keys[i].is_tombstone);
  }
}

}  // namespace log
}  // namespace mybitcask
