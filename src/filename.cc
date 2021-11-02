#include "filename.h"

#include <cassert>

namespace mybitcask {

const char* const LOG_FILE_SUFFIX = "log";
const char* const HINT_FILE_SUFFIX = "hint";
const char* const FILE_NAME_FORMAT = "%llu.%s";

static std::string MakeFileName(uint64_t number, const char* suffix) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), FILE_NAME_FORMAT,
                static_cast<unsigned long long>(number), suffix);
  return buf;
}

std::string LogFileName(uint64_t number) {
  assert(number >= 0);
  return MakeFileName(number, LOG_FILE_SUFFIX);
}

std::string HintFileName(uint64_t number) {
  assert(number >= 0);
  return MakeFileName(number, HINT_FILE_SUFFIX);
}

bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type) {
  char suffix[10];
  if (std::sscanf(filename.c_str(), FILE_NAME_FORMAT, number, suffix) < 2) {
    return false;
  }

  // The maximum number of digits in uint64_t cannot exceed 20
  if (filename.size() - std::strlen(suffix) - 1 > 20) {
    return false;
  }

  if (std::strcmp(suffix, LOG_FILE_SUFFIX) == 0) {
    *type = FileType::LogFile;
  } else if (std::strcmp(suffix, HINT_FILE_SUFFIX) == 0) {
    *type = FileType::HintFile;
  } else {
    return false;
  }

  return true;
}

}  // namespace mybitcask
