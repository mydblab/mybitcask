#include <string>
#include <filesystem>

#include "absl/status/statusor.h"

namespace mybitcask {
namespace test {

// returns a unique temp filename
absl::StatusOr<std::filesystem::path> TempFilename(std::string&& prefix = "",
                                                   std::string&& suffix = "",
                                                   std::size_t size = 12);

// Generates a random string of the specified number of size
template <class Engine>
std::string GenerateRandomString(Engine& engine, std::size_t size);

}  // namespace test
}  // namespace mybitcask
