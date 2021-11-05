#include "absl/status/statusor.h"
#include "ghc/filesystem.hpp"

#include <string>

namespace mybitcask {
namespace test {

// Class TempFile represents a temporary file,
// which is created file when TempFile is constructed
// and automatically deleted when TempFile is destructed
class TempFile {
 public:
  TempFile(ghc::filesystem::path&& filename);
  ~TempFile();

  const ghc::filesystem::path& Filename() const;

 private:
  ghc::filesystem::path filename_;
};

// Create a temporary file
absl::StatusOr<TempFile> MakeTempFile(std::string&& prefix = "",
                                    std::string&& suffix = "",
                                    std::size_t size = 12) noexcept;
// returns a unique temp filename
absl::StatusOr<ghc::filesystem::path> TempFilename(
    std::string&& prefix = "", std::string&& suffix = "",
    std::size_t size = 12) noexcept;

// Generates a random string of the specified number of size
template <class Engine>
std::string GenerateRandomString(Engine& engine, std::size_t size) noexcept;

}  // namespace test
}  // namespace mybitcask
