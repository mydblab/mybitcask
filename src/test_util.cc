#include "test_util.h"

#include <random>

namespace mybitcask {
namespace test {

const ghc::filesystem::path& TempFile::Filename() const { return filename_; }

TempFile::TempFile(ghc::filesystem::path&& filename)
    : filename_(std::forward<ghc::filesystem::path>(filename)) {
  std::ofstream f(filename_);
  f.close();
}

TempFile::~TempFile() { ghc::filesystem::remove(filename_); }

absl::StatusOr<TempFile> MakeTempFile(std::string&& prefix,
                                    std::string&& suffix,
                                    std::size_t size) noexcept {
  auto temp_filename = TempFilename(std::forward<std::string>(prefix),
                                    std::forward<std::string>(suffix), size);
  if (!temp_filename.ok()) {
    return absl::Status(temp_filename.status());
  }
  return TempFile(std::move(temp_filename.value()));
}

absl::StatusOr<ghc::filesystem::path> TempFilename(std::string&& prefix,
                                                   std::string&& suffix,
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
