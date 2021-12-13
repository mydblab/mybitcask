#ifndef MYBITCASK_SRC_TIMER_H_
#define MYBITCASK_SRC_TIMER_H_

#include <atomic>
#include <chrono>
#include <thread>
#include "iostream"

namespace mybitcask {
namespace timer {

// The SetInterval() method call the specified function at specified intervals.
// Returned a function to stop execution.
template <class Rep, class Period>
std::function<void()> SetInterval(
    std::function<void()> func,
    const std::chrono::duration<Rep, Period>& interval) {
  auto* active = new std::atomic_bool(true);
  std::thread t([=]() {
    while (active->load(std::memory_order_acquire)) {
      std::this_thread::sleep_for<Rep, Period>(interval);
      if (!active->load(std::memory_order_acquire)) {
        return;
      }
      func();
    }
  });

  t.detach();
  return [=]() {
    active->store(false, std::memory_order_release);
    delete active;
  };
}

}  // namespace timer
}  // namespace mybitcask
#endif  // MYBITCASK_SRC_TIMER_H_
