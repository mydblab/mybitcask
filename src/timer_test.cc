#include "timer.h"
#include "gtest/gtest.h"

namespace mybitcask {
namespace timer {
TEST(Timer, SetInterval) {
  auto c = 0;
  auto stop = SetInterval([&]() { c++; }, std::chrono::milliseconds(200));
  std::this_thread::sleep_for(std::chrono::milliseconds(420));
  stop();
  ASSERT_EQ(c, 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_EQ(c, 2);
}
}  // namespace timer
}  // namespace mybitcask
