#include "mybitcask/mybitcask.h"
#include "test_util.h"

#include <algorithm>
#include "gtest/gtest.h"

namespace mybitcask {

TEST(MyBitcaskTest, TestOpen) {
  auto tmpdir = test::MakeTempDir("mybitcask_");
  ASSERT_TRUE(tmpdir.ok());
  int entries_size = 100;
  auto entries = test::RandomEntries(entries_size);
  entries.erase(std::unique(entries.begin(), entries.end(),
                            [](test::TestEntry& a, test::TestEntry& b) {
                              return a.key == b.key;
                            }),
                entries.end());
  {
    auto mybitcask_status = Open(tmpdir->path(), 2048, false);
    ASSERT_TRUE(mybitcask_status.ok());
    std::unique_ptr<MyBitcask> mybitcask = std::move(mybitcask_status).value();
    for (auto& entry : entries) {
      if (entry.value.has_value()) {
        ASSERT_TRUE(mybitcask->Insert(entry.key, *entry.value).ok());
      }
    }
  }

  // reopen
  {
    auto mybitcask_status = Open(tmpdir->path(), 2048, false);
    ASSERT_TRUE(mybitcask_status.ok());
    std::unique_ptr<MyBitcask> mybitcask = std::move(mybitcask_status).value();

    for (auto& entry : entries) {
      if (entry.value.has_value()) {
        std::string value;
        auto exist = mybitcask->Get(absl::string_view(entry.key), &value);
        ASSERT_TRUE(exist.ok());
        ASSERT_TRUE(*exist);
        EXPECT_EQ(value, entry.value.value());
      }
    }
  }
}
}  // namespace mybitcask
