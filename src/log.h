#ifndef MYBITCASK_SRC_LOG_H_
#define MYBITCASK_SRC_LOG_H_

#include "absl/status/statusor.h"
#include "io.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mybitcask {
namespace log {

// Log entry format:
//              
//          |<--------- CRC coverage ---------->|
//              +---- size of ----+
//              |                 |
//              |                 v             
// +--------+---+--+--------+-- - - --+-- - - --+
// | CRC32C |key_sz| val_sz |   key   |   val   |
// +--------+------+----+---+-- - - --+-- - - --+
//  (32bits) (8bits) (16bits)               ^
//                      |                   |
//                      +----- size of -----+

// When the val_sz field is kTombstone(0xFFFF), it means delete the key
const uint16_t kTombstone = 0xFFFF;

const size_t kCrc32Len = 32;
const size_t kKeySzLen = 8;
const size_t kValSzLen = 16;
const size_t kHeaderLen = kCrc32Len + kKeySzLen + kValSzLen;

// invalid log entry.
// Currently there are two situations in which this error will occur:
// * The entry has an invalid CRC
// * The entry has wrong length
const std::string kErrBadEntry = "bad entry";

struct Entry {
  const std::vector<const uint8_t> key;
  const std::vector<const uint8_t> value;
};

class LogReader {
 public:
  LogReader(io::RandomAccessReader* reader);
  absl::StatusOr<std::optional<Entry>> Read(uint64_t offset) noexcept;

 private:
  io::RandomAccessReader* reader_;
};

}  // namespace log
}  // namespace mybitcask

#endif  // MYBITCASK_LOG_H_
