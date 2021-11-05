#include "absl/status/statusor.h"
#include "ghc/filesystem.hpp"

#include <string>

namespace mybitcask {
namespace test {

// returns a unique temp filename
absl::StatusOr<ghc::filesystem::path> TempFilename(
    std::string&& prefix = "", std::string&& suffix = "",
    std::size_t size = 12) noexcept;

// Generates a random string of the specified number of size
template <class Engine>
std::string GenerateRandomString(Engine& engine, std::size_t size) noexcept;

}  // namespace test
}  // namespace mybitcask
