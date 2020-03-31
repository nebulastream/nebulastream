#ifndef NES_THREADBARRIER_HPP
#define NES_THREADBARRIER_HPP

namespace NES {
class ThreadBarrier {
  public:
    explicit ThreadBarrier(uint32_t size) : size(size), count(0), mutex(), cvar() {}

    ThreadBarrier(const ThreadBarrier&) = delete;

    ThreadBarrier& operator=(const ThreadBarrier&) = delete;

    void wait() {
        std::unique_lock<std::mutex> lock(mutex);
        if (++count >= size) {
            cvar.notify_all();
        } else {
            while (count < size) {
                cvar.wait(lock);
            }
        }
    }

  private:
    const uint32_t size;
    std::mutex mutex;
    uint32_t count;
    std::condition_variable cvar;
};
}
#endif //NES_THREADBARRIER_HPP
