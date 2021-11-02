#include "store_filename.h"

#include <gtest/gtest.h>
#include <vector>

namespace mybitcask {
namespace store {
struct FilenameInfo {
  std::string filename;
  io::file_id_t file_id;
  FileType type;
};

TEST(FileNameTest, FileName) {
  std::vector<FilenameInfo> cases = {
      {"100.log", 100, FileType::kLogFile},
      {"0.log", 0, FileType::kLogFile},
      {"0.hint", 0, FileType::kHintFile},
      {"4294967295.hint", 4294967295, FileType::kHintFile}};

  for (auto filenameInfo : cases) {
    if (filenameInfo.type == FileType::kLogFile) {
      EXPECT_EQ(LogFileName(filenameInfo.file_id), filenameInfo.filename);
    } else {
      EXPECT_EQ(HintFileName(filenameInfo.file_id), filenameInfo.filename);
    }
  }
}
TEST(FileNameTest, Parse) {
  std::vector<FilenameInfo> cases = {
      {"100.log", 100, FileType::kLogFile},
      {"0.log", 0, FileType::kLogFile},
      {"0.hint", 0, FileType::kHintFile},
  };
  io::file_id_t file_id;
  FileType type;

  for (auto filenameInfo : cases) {
    EXPECT_TRUE(ParseFileName(filenameInfo.filename, &file_id, &type));
    EXPECT_EQ(file_id, filenameInfo.file_id);
    EXPECT_EQ(type, filenameInfo.type);
  }
  std::vector<std::string> errors = {"",     "foo",     "foo-dx-100.log",
                                     ".log", ".hint",   "100",
                                     "100.", "100.abc", "42949672951.hint"};
  for (auto filename : errors) {
    EXPECT_FALSE(ParseFileName(filename, &file_id, &type));
  }
}
}  // namespace store
}  // namespace mybitcask
