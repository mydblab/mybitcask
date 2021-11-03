#include "store.h"
#include "store_filename.h"
namespace mybitcask {
namespace store {

// Store Store::open(std::filesystem::path path) {
//   if (!std::filesystem::exists(path)) {
//     std::filesystem::create_directory(path);
//   }
// }

void Store::Append(const char* src, const size_t size) {
  auto file_size = std::filesystem::file_size(path_.append(
      LogFileName(cur_max_file_id_.load(std::memory_order_acquire))));

  if (file_size >= dead_bytes_threshold_) {
    writer_->close();
    writer_ = New_log_file();
  }

  writer_->write(src, size);
}

Reader Store::GetReader() { return Reader(this); }

std::ofstream* Store::New_log_file() {
  std::ofstream* file = new std::ofstream(path_.append(
      LogFileName(cur_max_file_id_.fetch_add(std::memory_order_release))));
  return file;
}

Store::Store(std::filesystem::path path)
    : path_(path),
      dead_bytes_threshold_(kDefaultDeadBytesThreshold),
      writer_(nullptr),
      cur_max_file_id_(0) {}

Store::Store(std::filesystem::path path, size_t dead_bytes_threshold)
    : path_(path),
      dead_bytes_threshold_(dead_bytes_threshold),
      writer_(nullptr),
      cur_max_file_id_(0) {}

Store::~Store() {
  if (nullptr != writer_) {
    writer_->close();
  }
}

size_t Reader::Read(char* const dst, const size_t size) {
  auto file = File(cur_file_id_);
  if (nullptr == file) {
    // error
    return 0;
  }
  auto read_count = file->read(dst, size).gcount();
  if (file->eof()) {
    cur_file_id_++;
  }
  return read_count;
}

void Reader::Seek(const io::Position& pos) {
  auto file = File(pos.file_id);
  if (nullptr == file) {
    // error
    return;
  }

  file->seekg(pos.offset_in_file);
  cur_file_id_ = pos.file_id;
}

Reader::Reader(Store* store, io::file_id_t cur_file_id)
    : store_(store), cur_file_id_(cur_file_id), files_({}) {}

Reader::Reader(Store* store) : store_(store), cur_file_id_(0), files_({}) {}

Reader::~Reader() {
  for (const auto& [_, file] : files_) {
    file->close();
  }
}

std::ifstream* Reader::File(io::file_id_t file_id) {
  auto it = files_.find(file_id);
  if (it == files_.end()) {
    if (store_->cur_max_file_id_.load(std::memory_order_acquire) < file_id) {
      return nullptr;
    }
    std::ifstream* file =
        new std::ifstream(store_->path_.append(LogFileName(file_id)));
    files_.insert({file_id, file});
    return file;
  }
  return it->second;
}
}  // namespace store
}  // namespace mybitcask
