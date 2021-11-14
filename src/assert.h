#ifndef MYBITCASK_SRC_ASSERT_H_
#define MYBITCASK_SRC_ASSERT_H_

#include <cassert>

// Use (void) to silent unused warnings.
#define assertm(exp, msg) assert(((void)msg, exp))

#endif  // MYBITCASK_SRC_ASSERT_H_
