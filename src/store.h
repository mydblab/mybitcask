#ifndef MYBITCASK_SRC_STORE_H_
#define MYBITCASK_SRC_STORE_H_

#include "io.h"

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <unordered_map>

namespace mybitcask {
namespace store {

// default dead bytes threshold (128MB)
const size_t kDefaultDeadBytesThreshold = 128 * 1024 * 1024;

class Reader;

class Store {
 public:
  void Append(const char* src, const size_t size);

  // Opens a `Store` with the given path.
  // This will create a new directory if the given one does not exist.
  // static Store open(std::filesystem::path path);

  Store() = delete;
  ~Store();

  Reader GetReader();

 private:
  std::ofstream* New_log_file();

  Store(std::filesystem::path path);
  Store(std::filesystem::path path, size_t dead_bytes_threshold);

  std::atomic_uint cur_max_file_id_;
  const size_t dead_bytes_threshold_;
  std::filesystem::path path_;
  std::ofstream* writer_;

  friend class Reader;
};

class Reader {
 public:
  size_t Read(char* const dst, const size_t size);
  void Seek(const io::Position& pos);

  Reader() = delete;
  ~Reader();

 private:
  Reader(Store* store, io::file_id_t cur_file_id);
  Reader(Store* store);

  std::ifstream* File(io::file_id_t file_id);

  std::unordered_map<io::file_id_t, std::ifstream*> files_;
  io::file_id_t cur_file_id_;
  Store* store_;

  friend class Store;
};

}  // namespace store
}  // namespace mybitcask

#endif  // MYBITCASK_SRC_STORE_H_
