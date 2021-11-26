#include "store_hint.h"

#include "absl/status/statusor.h"

namespace mybitcask {
namespace store {

namespace hint {

absl::Status Generate(absl::string_view hint_filepath) noexcept {
  return absl::Status();
}

template <typename T>
absl::StatusOr<T> FoldKeys(absl::string_view hint_filepath, T init,
                           std::function<T(T&, Entry&&)> f) {
  std::ifstream file(hint_filepath, std::ios::binary | std::ios::in);

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
    entry.key.resize(entry.val_sz);
    file.read(reinterpret_cast<const std::uint8_t*>(entry.key.data()),
              entry.val_sz);
    if (file.fail()) {
      return absl::InternalError(kErrRead);
    }
    f(acc, std::move(entry));
  }
}

}  // namespace hint
}  // namespace store
}  // namespace mybitcask
