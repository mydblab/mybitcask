#include "filename.h"

#include <gtest/gtest.h>
#include <vector>

namespace mybitcask {

struct FilenameInfo {
  std::string filename;
  uint64_t number;
  FileType type;
};

TEST(FileNameTest, FileName) {
  std::vector<FilenameInfo> cases = {
      {"100.log", 100, FileType::LogFile},
      {"0.log", 0, FileType::LogFile},
      {"0.hint", 0, FileType::HintFile},
      {"18446744073709551000.hint", 18446744073709551000ull,
       FileType::HintFile}};

  for (auto filenameInfo : cases) {
    if (filenameInfo.type == FileType::LogFile) {
      EXPECT_EQ(LogFileName(filenameInfo.number), filenameInfo.filename);
    } else {
      EXPECT_EQ(HintFileName(filenameInfo.number), filenameInfo.filename);
    }
  }
}
TEST(FileNameTest, Parse) {
  std::vector<FilenameInfo> cases = {
      {"100.log", 100, FileType::LogFile},
      {"0.log", 0, FileType::LogFile},
      {"0.hint", 0, FileType::HintFile},
  };
  uint64_t number;
  FileType type;

  for (auto filenameInfo : cases) {
    EXPECT_TRUE(ParseFileName(filenameInfo.filename, &number, &type));
    EXPECT_EQ(number, filenameInfo.number);
    EXPECT_EQ(type, filenameInfo.type);
  }
  std::vector<std::string> errors = {
      "",     "foo",     "foo-dx-100.log",
      ".log", ".hint",   "100",
      "100.", "100.abc", "184467440737095520001.hint"};
  for (auto filename : errors) {
    EXPECT_FALSE(ParseFileName(filename, &number, &type));
  }
}
}  // namespace mybitcask
