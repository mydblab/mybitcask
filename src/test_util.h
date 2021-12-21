#ifndef MYBITCASK_SRC_TEST_UTIL_H_
#define MYBITCASK_SRC_TEST_UTIL_H_

#include "absl/status/statusor.h"
#include "ghc/filesystem.hpp"
#include "mybitcask/mybitcask.h"

#include <string>

namespace mybitcask {

namespace log {
class Writer;
}

namespace test {

// Class TempFile represents a temporary file,
// which is created file when TempFile is constructed
// and automatically deleted when TempFile is destructed
class TempFile {
 public:
  TempFile(ghc::filesystem::path&& filename);

  TempFile(TempFile&& other) noexcept;
  TempFile& operator=(TempFile&&) noexcept;

  TempFile(const TempFile&) = delete;
  TempFile& operator=(const TempFile&) = delete;

  ~TempFile();

  const ghc::filesystem::path& path() const;

 private:
  ghc::filesystem::path path_;
};

// Create a temporary file
absl::StatusOr<TempFile> MakeTempFile(std::string&& prefix = "",
                                      std::string&& suffix = "",
                                      std::size_t name_length = 12) noexcept;

// Create a temporary directory
absl::StatusOr<TempFile> MakeTempDir(std::string&& prefix = "",
                                     std::size_t name_length = 12) noexcept;

// returns a unique temp filename
absl::StatusOr<ghc::filesystem::path> TempFilename(
    std::string&& prefix = "", std::string&& suffix = "",
    std::size_t name_length = 12) noexcept;

// Generates a random bool value
bool RandomBool() noexcept;
template <class Engine>
bool RandomBool(Engine& engine) noexcept;

// Generates a random string of the specified length
std::string RandomString(std::size_t length) noexcept;
template <class Engine>
std::string RandomString(Engine& engine, std::size_t length) noexcept;

// Generate a random of the specified length range
std::string RandomString(std::size_t min_len, std::size_t max_len) noexcept;
template <class Engine>
std::string RandomString(Engine& engine, std::size_t min_len,
                         std::size_t max_len) noexcept;

// Converting string_view to span<uint8_t>
absl::Span<std::uint8_t> StrSpan(absl::string_view buf) noexcept;

struct TestEntry {
  std::string key;
  // Tombstone if None.
  absl::optional<std::string> value;
};

// Generate a random entry
TestEntry RandomEntry();

absl::Status AppendTestEntry(
    log::Writer* w, const TestEntry& test_entry,
    const std::function<void(Position)>& success_callback = [](Position) {});

}  // namespace test
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_TEST_UTIL_H_
