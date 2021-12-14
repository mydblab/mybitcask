#include "store_hint.h"
#include "store_filename.h"

#include "absl/status/statusor.h"

namespace mybitcask {
namespace store {

namespace hint {

Generator::Generator(log::Reader* log_reader, const ghc::filesystem::path& path)
    : log_reader_(log_reader), path_(path) {}

absl::Status Generator::Generate(std::uint32_t file_id) noexcept {
  std::ifstream hint_file(path_ / HintFilename(file_id),
                          std::ios::binary | std::ios::trunc | std::ios::out);
  if (!hint_file) {
    return absl::InternalError(kErrRead);
  }

  return absl::Status();
}

template <typename T>
absl::StatusOr<T> FoldKeys(absl::string_view hint_filepath, T init,
                           std::function<T(T&&, Entry&&)> f) noexcept {
  std::ifstream file(hint_filepath.data(), std::ios::binary | std::ios::in);

  if (!file) {
    return absl::InternalError(kErrRead);
  }
  auto acc = std::move(init);
  while (true) {
    Entry entry{};
    file >> entry.key_sz >> entry.entry_sz >> entry.offset;
    if (file.eof()) {
      return acc;
    }
    if (file.fail()) {
      return absl::InternalError(kErrRead);
    }
    entry.key.resize(entry.key_sz);
    file.read(reinterpret_cast<char*>(entry.key.data()), entry.key_sz);
    if (file.fail()) {
      return absl::InternalError(kErrRead);
    }
    acc = f(std::move(acc), std::move(entry));
  }
  return acc;
}

}  // namespace hint
}  // namespace store
}  // namespace mybitcask
