#include "test_util.h"

#include <random>

namespace mybitcask {
namespace test {

absl::StatusOr<ghc::filesystem::path> TempFilename(const std::string&& prefix,
                                                   const std::string&& suffix,
                                                   std::size_t size) noexcept {
  std::error_code ec;
  auto tmpdir = ghc::filesystem::temp_directory_path(ec).parent_path();
  if (ec) {
    return absl::InternalError(ec.message());
  }

  std::default_random_engine e(std::random_device{}());
  ghc::filesystem::path filename{};
  while (true) {
    filename =
        tmpdir /
        (prefix + GenerateRandomString<std::default_random_engine>(e, size) +
         suffix);
    if (!ghc::filesystem::exists(filename)) {
      break;
    }
  }
  return filename;
}

template <class Engine>
std::string GenerateRandomString(Engine& engine, std::size_t size) noexcept {
  std::uniform_int_distribution<short> dist(0, 35);
  std::string str;
  str.reserve(size);

  for (size_t i = 0; i < size; ++i) {
    short val = dist(engine);
    if (val < 26) {
      str.push_back('a' + char(val));
    } else {
      str.push_back('0' + char(val - 26));
    }
  }
  return str;
}

}  // namespace test
}  // namespace mybitcask
