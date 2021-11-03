#ifndef MYBITCASK_SRC_LOG_H_
#define MYBITCASK_SRC_LOG_H_

#include <cstdint>

namespace mybitcask {
namespace log {

struct Entry {
  uint16_t key_size;
  uint16_t value_size;
  const uint8* key;
  const uint8* value;
};

// Log entry format:
//
//               +---- size of ----+
//               |                 |
//               |                 v
// +--------+----+---+--------+-- - - --+-- - - --+
// | CRC32C | key_sz | val_sz |   key   |   val   |
// +--------+--------+---+----+-- - - --+-- - - --+
//  (32bits) (16bits) (16bits)               ^
//                       |                   |
//                       +----- size of -----+

}  // namespace log
}  // namespace mybitcask

#endif  // MYBITCASK_LOG_H_
