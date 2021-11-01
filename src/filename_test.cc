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
  };

  for (auto filename_info : cases) {
    if (filename_info.type == FileType::LogFile) {
      EXPECT_EQ(LogFileName(filename_info.number), filename_info.filename);
    } else {
      EXPECT_EQ(HintFileName(filename_info.number), filename_info.filename);
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

  for (auto filename_info : cases) {
    EXPECT_TRUE(ParseFileName(filename_info.filename, &number, &type));
    EXPECT_EQ(number, filename_info.number);
    EXPECT_EQ(type, filename_info.type);
  }
  std::vector<std::string> errors = {
      "", "foo", "foo-dx-100.log", ".log", ".hint", "100", "100.", "100.abc"};
  for (auto filename : errors) {
    EXPECT_FALSE(ParseFileName(filename, &number, &type));
  }
}
}  // namespace mybitcask
