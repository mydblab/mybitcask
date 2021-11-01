#ifndef MYBITCASK_SRC_FILENAME_H_
#define MYBITCASK_SRC_FILENAME_H_

#include <cstdint>
#include <string>

namespace mybitcask {

enum FileType { LogFile, HintFile };

// Return the name of the log file with the specified number
std::string LogFileName(uint64_t number);

// Return the name of the hint file with the specified number
std::string HintFileName(uint64_t number);

// If filename is a mybitcask file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type);

}  // namespace mybitcask

#endif  // MYBITCASK_SRC_FILENAME_H_
