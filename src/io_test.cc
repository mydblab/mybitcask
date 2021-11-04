

#include "io.h"

#include <gtest/gtest.h>
#include <cstdio>
#include <filesystem>
#include <iostream>

namespace mybitcask {
namespace io {

TEST(IoTest, FStreamSequentialWriter) {
  std::cout << std::tmpfile() << std::endl;

  // std::string tmpfilename = std::tmpnam();
  // auto writer = OpenSequentialWriter(tmpfilename);
  // ASSERT_TRUE(writer.ok());
  // char* testdata = "test data.";
  // (*writer)->Append(
  //     absl::Span(reinterpret_cast<std::uint8_t*>(testdata), sizeof
  //     testdata));
  // (*writer)->Sync();
  // (*writer).release();
}

}  // namespace io
}  // namespace mybitcask
