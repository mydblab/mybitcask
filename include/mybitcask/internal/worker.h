#ifndef MYBITCASK_SRC_WORKER_H_
#define MYBITCASK_SRC_WORKER_H_
namespace mybitcask {
namespace worker {

class Worker {
 public:
  virtual void Start(std::size_t interval_seconds) = 0;

  virtual ~Worker() = default;
};

}  // namespace worker
}  // namespace mybitcask
#endif  // MYBITCASK_SRC_WORKER_H_
