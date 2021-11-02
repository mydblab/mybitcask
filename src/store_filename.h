#ifndef MYBITCASK_SRC_FILENAME_H_
#define MYBITCASK_SRC_FILENAME_H_

#include "io.h"

#include <string>

namespace mybitcask {
namespace store {
enum FileType { kLogFile, kHintFile };

// Return the name of the log file with the specified file_id
std::string LogFileName(io::file_id_t file_id);

// Return the name of the hint file with the specified file_id
std::string HintFileName(io::file_id_t file_id);

// If filename is a mybitcask file, store the type of the file in *type.
// The file_id encoded in the filename is stored in *file_id.  If the
// filename was successfully parsed, returns true.  Else return false.
bool ParseFileName(const std::string& filename, io::file_id_t* file_id,
                   FileType* type);

}  // namespace store
}  // namespace mybitcask
#endif  // MYBITCASK_SRC_FILENAME_H_
