#include "store_hint.h"
#include "mybitcask/internal/log.h"
#include "mybitcask/internal/store.h"
#include "store_dbfiles.h"
#include "test_util.h"

#include "gtest/gtest.h"

namespace mybitcask {
namespace store {
namespace hint {

TEST(HintTest, GenerateAndFold) {
  auto tmpdir = test::MakeTempDir("mybitcask_store_hint_");
  ASSERT_TRUE(tmpdir.ok());
  store::Store store(tmpdir->path(),
                     store::DBFiles(tmpdir->path()).latest_file_id(), 2048);
  log::Writer log_writer(&store);
  log::Reader log_reader(&store, false);

  std::vector<test::TestEntry> entries;
  int entries_size = 100;
  for (int i = 0; i < entries_size; i++) {
    auto entry = test::RandomEntry();
    entries.push_back(entry);
    ASSERT_TRUE(test::AppendTestEntry(&log_writer, entry).ok());
  }
  Generator gen(&log_reader, tmpdir->path());

  store::DBFiles dbfiles(tmpdir->path());
  for (auto file_id : dbfiles.active_log_files()) {
    ASSERT_TRUE(gen.Generate(file_id).ok());
  }
  struct Void {};
  struct TestKey {
    std::string key;
    bool is_tombstone;
  };
  std::vector<TestKey> fold_keys;
  dbfiles = store::DBFiles(tmpdir->path());
  for (auto file_id : dbfiles.hint_files()) {
    auto status =
        KeyIter(&tmpdir->path(), file_id)
            .Fold<Void, std::string>(
                Void(), [&](Void&&, log::Key<std::string>&& key) {
                  fold_keys.push_back(TestKey{std::move(key.key_data),
                                              !key.value_pos.has_value()});
                  return Void();
                });
    ASSERT_TRUE(status.ok());
  }

  ASSERT_EQ(entries.size(), fold_keys.size());
  for (int i = 0; i < fold_keys.size(); i++) {
    EXPECT_EQ(entries[i].key, fold_keys[i].key);
    EXPECT_EQ(!entries[i].value.has_value(), fold_keys[i].is_tombstone);
  }
}
}  // namespace hint
}  // namespace store

}  // namespace mybitcask
