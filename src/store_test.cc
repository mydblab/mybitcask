#include "store.h"

#include <gtest/gtest.h>

namespace mybitcask {
namespace store {

TEST(StoreTest, FStreamSequentialWriter) {
  std::string tmpfilename = std::tmpnam(nullptr);
  auto writer = FStreamSequentialWriter::Open(tmpfilename);
  ASSERT_TRUE(writer.ok());
  char* testdata = "test data.";
  (*writer)->Append(
      absl::Span(reinterpret_cast<std::uint8_t*>(testdata), sizeof testdata));
  (*writer)->Sync();
  (*writer).release();
}

}  // namespace store
}  // namespace mybitcask
