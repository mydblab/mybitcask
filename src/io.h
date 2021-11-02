#ifndef MYBITCASK_SRC_IO_H_
#define MYBITCASK_SRC_IO_H_
namespace mybitcask {
namespace io {

using file_id_t = uint32_t;

// Position represents a position in db files.
struct Position {
  file_id_t file_id;
  uint64_t offset_in_file;
};

// A reader abstraction for reading sequentially bytes
class SequentialReader {
 public:
  SequentialReader& operator=(const SequentialReader&) = delete;

  virtual ~SequentialReader();
};
}  // namespace io
}  // namespace mybitcask
#endif
