#include "test_util.h"
#include "mybitcask/internal/log.h"

#include <random>

namespace mybitcask {
namespace test {

const ghc::filesystem::path& TempFile::path() const { return path_; }

TempFile::TempFile(ghc::filesystem::path&& path) : path_(std::move(path)) {
  std::ofstream f(path_);
  f.close();
}

TempFile::TempFile(TempFile&& other) noexcept : path_(std::move(other.path_)) {}

TempFile& TempFile::operator=(TempFile&& other) noexcept {
  if (path_ != other.path_) {
    path_ = std::move(other.path_);
  }
  return *this;
}

TempFile::~TempFile() {
  if (!path_.empty()) {
    ghc::filesystem::remove_all(path_);
  }
}

absl::StatusOr<TempFile> MakeTempFile(std::string&& prefix,
                                      std::string&& suffix,
                                      std::size_t name_length) noexcept {
  auto temp_filename =
      TempFilename(std::move(prefix), std::move(suffix), name_length);
  if (!temp_filename.ok()) {
    return absl::Status(temp_filename.status());
  }
  return TempFile(std::move(temp_filename.value()));
}

absl::StatusOr<TempFile> MakeTempDir(std::string&& prefix,
                                     std::size_t name_length) noexcept {
  auto temp_dirname = TempFilename(std::move(prefix), "", name_length);
  if (!temp_dirname.ok()) {
    return absl::Status(temp_dirname.status());
  }
  std::error_code ec;
  auto create_ok = ghc::filesystem::create_directories(*temp_dirname, ec);
  if (ec) {
    return absl::InternalError(ec.message());
  }
  if (!create_ok) {
    return absl::InternalError("directory create failed");
  }
  return TempFile(std::move(temp_dirname.value()));
}

absl::StatusOr<ghc::filesystem::path> TempFilename(
    std::string&& prefix, std::string&& suffix,
    std::size_t name_length) noexcept {
  std::error_code ec;
  auto tmpdir = ghc::filesystem::temp_directory_path(ec).parent_path();
  if (ec) {
    return absl::InternalError(ec.message());
  }

  ghc::filesystem::path filename{};
  while (true) {
    filename = tmpdir / (prefix + RandomString(name_length) + suffix);
    if (!ghc::filesystem::exists(filename)) {
      break;
    }
  }
  return filename;
}

static std::default_random_engine _default_random_engine(
    std::random_device{}());

bool RandomBool() noexcept { return RandomBool(_default_random_engine); }

template <class Engine>
bool RandomBool(Engine& engine) noexcept {
  static auto gen = std::bind(std::uniform_int_distribution<>(0, 1), engine);
  return gen();
}

std::string RandomString(std::size_t length) noexcept {
  return RandomString(_default_random_engine, length);
}

std::string RandomString(std::size_t min_len, std::size_t max_len) noexcept {
  return RandomString(_default_random_engine, min_len, max_len);
}

template <class Engine>
std::string RandomString(Engine& engine, std::size_t length) noexcept {
  std::uniform_int_distribution<short> dist(0, 35);
  std::string str;
  str.reserve(length);

  for (size_t i = 0; i < length; ++i) {
    short val = dist(engine);
    if (val < 26) {
      str.push_back('a' + char(val));
    } else {
      str.push_back('0' + char(val - 26));
    }
  }
  return str;
}

template <class Engine>
std::string RandomString(Engine& engine, std::size_t min_len,
                         std::size_t max_len) noexcept {
  std::uniform_int_distribution<std::size_t> dist(min_len, max_len);
  return RandomString(engine, dist(engine));
}

absl::Span<std::uint8_t> StrSpan(absl::string_view buf) noexcept {
  return {reinterpret_cast<std::uint8_t*>(const_cast<char*>(buf.data())),
          buf.size()};
}

TestEntry RandomEntry() {
  auto is_tombstone = RandomBool();

  absl::optional<std::string> value;
  if (is_tombstone) {
    value = absl::nullopt;
  } else {
    value = RandomString(0x1, 0xF0);
  }
  return TestEntry{RandomString(0x1, 0xF0), value};
}

std::vector<TestEntry> RandomEntries(std::size_t n) {
  std::vector<TestEntry> ret;
  ret.reserve(n);
  for (int i = 0; i < n; i++) {
    ret.push_back(RandomEntry());
  }
  return ret;
}

absl::Status AppendTestEntry(
    log::Writer* w, const TestEntry& test_entry,
    const std::function<void(Position)>& success_callback) {
  if (test_entry.value.has_value()) {
    return w->Append(StrSpan(test_entry.key), StrSpan(*test_entry.value),
                     success_callback);
  } else {
    return w->AppendTombstone(StrSpan(test_entry.key), success_callback);
  }
}

}  // namespace test
}  // namespace mybitcask
