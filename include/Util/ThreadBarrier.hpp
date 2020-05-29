#ifndef NES_THREADBARRIER_HPP
#define NES_THREADBARRIER_HPP

namespace NES {

/**
 * @brief Utility class that introduce a barrier for N threads.
 * The barrier resets when N threads call wait().
 */
class ThreadBarrier {
  public:
    /**
     * @brief Create a Barrier for size threads
     * @param size
     */
    explicit ThreadBarrier(uint32_t size) : size(size), count(0), mutex(), cvar() {}

    ThreadBarrier(const ThreadBarrier&) = delete;

    ThreadBarrier& operator=(const ThreadBarrier&) = delete;

    /**
     * @brief This method will block the calling thread until N threads have invoke wait().
     */
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
}// namespace NES
#endif//NES_THREADBARRIER_HPP
