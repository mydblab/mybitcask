#include "absl/status/statusor.h"
#include "ghc/filesystem.hpp"

#include <string>

namespace mybitcask {
namespace test {

// returns a unique temp filename
absl::StatusOr<ghc::filesystem::path> TempFilename(
    const std::string&& prefix = "", const std::string&& suffix = "",
    std::size_t size = 12);

// Generates a random string of the specified number of size
template <class Engine>
std::string GenerateRandomString(Engine& engine, std::size_t size);

}  // namespace test
}  // namespace mybitcask
