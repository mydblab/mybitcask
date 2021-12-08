#include "store_filename.h"

#include <cassert>

namespace mybitcask {
namespace store {
const char* const LOG_FILE_SUFFIX = "log";
const char* const HINT_FILE_SUFFIX = "hint";
const char* const FILENAME_FORMAT = "%u.%s";

static std::string MakeFilename(std::uint32_t file_id, const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), FILENAME_FORMAT,
                static_cast<unsigned int>(file_id), suffix);
  return buf;
}

std::string LogFilename(std::uint32_t file_id) {
  assert(file_id >= 0);
  return MakeFilename(file_id, LOG_FILE_SUFFIX);
}

std::string HintFilename(std::uint32_t file_id) {
  assert(file_id >= 0);
  return MakeFilename(file_id, HINT_FILE_SUFFIX);
}

bool ParseFilename(absl::string_view filename, std::uint32_t* file_id,
                   FileType* type) {
  char suffix[10]{};
  suffix[9] = '\0';
  if (std::sscanf(filename.data(), FILENAME_FORMAT, file_id, suffix) < 2) {
    return false;
  }

  // The maximum number of digits in std::uint32_t cannot exceed 10
  if (filename.size() - std::strlen(suffix) - 1 > 10) {
    return false;
  }

  if (std::strcmp(suffix, LOG_FILE_SUFFIX) == 0) {
    *type = FileType::kLogFile;
  } else if (std::strcmp(suffix, HINT_FILE_SUFFIX) == 0) {
    *type = FileType::kHintFile;
  } else {
    return false;
  }

  return true;
}
}  // namespace store
}  // namespace mybitcask
