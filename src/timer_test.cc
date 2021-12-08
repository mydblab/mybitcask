#include "timer.h"
#include "gtest/gtest.h"

#include <chrono>

namespace mybitcask {
namespace timer {
TEST(Timer, SetInterval) {
  auto c = 1;
  auto stop = SetInterval([&]() { c++; }, std::chrono::milliseconds(100));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop();
  ASSERT_EQ(c, 5);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(c, 5);
}
}  // namespace timer
}  // namespace mybitcask
