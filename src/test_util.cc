#include "test_util.h"

#include <filesystem>
#include <random>

namespace mybitcask {
namespace test {

std::string TempFilename(std::string_view prefix = "") {
  // auto tmpdir = std::filesystem::temp_directory_path();
  // std::default_random_engine e(std::random_device());
}

}  // namespace test
}  // namespace mybitcask
