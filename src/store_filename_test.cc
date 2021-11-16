#include "store_filename.h"
#include "gtest/gtest.h"

#include <vector>

namespace mybitcask {
namespace store {
struct FilenameInfo {
  std::string filename;
  std::uint32_t file_id;
  FileType type;
};

TEST(FilenameTest, Filename) {

  std::vector<FilenameInfo> cases = {
      {"100.log", 100, FileType::kLogFile},
      {"0.log", 0, FileType::kLogFile},
      {"0.hint", 0, FileType::kHintFile},
      {"4294967295.hint", 4294967295, FileType::kHintFile}};

  for (const auto& filenameInfo : cases) {
    if (filenameInfo.type == FileType::kLogFile) {
      EXPECT_EQ(LogFilename(filenameInfo.file_id), filenameInfo.filename);
    } else {
      EXPECT_EQ(HintFilename(filenameInfo.file_id), filenameInfo.filename);
    }
  }
}

TEST(FilenameTest, Parse) {
  std::vector<FilenameInfo> cases = {
      {"100.log", 100, FileType::kLogFile},
      {"0.log", 0, FileType::kLogFile},
      {"0.hint", 0, FileType::kHintFile},
  };
  std::uint32_t file_id = 0;
  FileType type{};

  for (const auto& filenameInfo : cases) {
    EXPECT_TRUE(ParseFilename(filenameInfo.filename, &file_id, &type));
    EXPECT_EQ(file_id, filenameInfo.file_id);
    EXPECT_EQ(type, filenameInfo.type);
  }
  std::vector<std::string> errors = {"",     "foo",     "foo-dx-100.log",
                                     ".log", ".hint",   "100",
                                     "100.", "100.abc", "42949672951.hint"};
  for (const auto& filename : errors) {
    EXPECT_FALSE(ParseFilename(filename, &file_id, &type));
  }
}
}  // namespace store
}  // namespace mybitcask
