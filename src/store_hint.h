#ifndef MYBITCASK_SRC_STORE_HINT_H_
#define MYBITCASK_SRC_STORE_HINT_H_

#include "mybitcask/internal/store.h"
#include "mybitcask/mybitcask.h"

#include <cstdint>
#include <functional>
#include <vector>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace mybitcask {
namespace store {
namespace hint {

const std::string kErrRead = "failed to read hint entry";

struct Entry {
  std::uint32_t key_sz;
  std::uint32_t entry_sz;
  std::uint32_t offset;
  std::vector<std::uint8_t> key;
};

absl::Status Generate(absl::string_view hint_filepath) noexcept;

template <typename T>
absl::StatusOr<T> FoldKeys(absl::string_view hint_filepath, T init,
                           std::function<T(T, Entry)> f) noexcept;

}  // namespace hint
}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_HINT_H_
