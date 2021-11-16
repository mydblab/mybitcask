#include "mybitcask/internal/store.h"
#include "gtest/gtest.h"
#include "test_util.h"

namespace mybitcask {
namespace store {
TEST(Store, StoreReaderWriter) {
  auto tmpdir = test::MakeTempDir("mybitcask_store_");
  ASSERT_TRUE(tmpdir.ok());
  {
    LogFiles log_files(tmpdir->path());
    Store store(log_files, 10);
    auto status = store.Append(test::StrSpan("1111"));
    ASSERT_TRUE(status.ok());
    status = store.Sync();
    ASSERT_TRUE(status.ok());
    status = store.Append(test::StrSpan("2222"));
    ASSERT_TRUE(status.ok());
    status = store.Sync();
    ASSERT_TRUE(status.ok());
    status = store.Append(test::StrSpan("3333"));
    ASSERT_TRUE(status.ok());
    status = store.Sync();
    ASSERT_TRUE(status.ok());
  }
  // Open from disk again and check persistent data.
  {
    LogFiles log_files(tmpdir->path());
    Store store(log_files, 10);
    std::uint8_t buf[5]{};
    buf[4] = '\0';
    auto buf_len = sizeof(buf) - 1;
    auto actual_size = store.ReadAt(Position(1, 0), {buf, buf_len});
    ASSERT_TRUE(actual_size.ok());
    ASSERT_EQ(*actual_size, buf_len);
    ASSERT_STREQ(reinterpret_cast<char*>(buf), "1111");

    actual_size = store.ReadAt(Position(1, 4), {buf, buf_len});
    ASSERT_TRUE(actual_size.ok());
    ASSERT_EQ(*actual_size, buf_len);
    ASSERT_STREQ(reinterpret_cast<char*>(buf), "2222");

    actual_size = store.ReadAt(Position(2, 0), {buf, buf_len});
    ASSERT_TRUE(actual_size.ok());
    ASSERT_EQ(*actual_size, buf_len);
    ASSERT_STREQ(reinterpret_cast<char*>(buf), "3333");
  }
}
}  // namespace store
}  // namespace mybitcask
