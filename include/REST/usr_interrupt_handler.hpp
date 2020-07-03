#include <condition_variable>
#include <iostream>
#include <mutex>
#include <signal.h>

static std::condition_variable _condition;
static std::mutex _mutex;

namespace NES {

class InterruptHandler {
  public:
    static void hookUserInterruptHandler() {
        signal(SIGTERM, handleUserInterrupt);
    }

    static void handleUserInterrupt(int signal) {
        std::cout << "handleUserInterrupt" << '\n';
        if (signal == SIGTERM) {
            std::cout << "SIGINT trapped ..." << '\n';
            _condition.notify_one();
        }
    }

    static void waitForUserInterrupt() {
        std::cout << "waitForUserInterrupt()" << '\n';
        std::unique_lock<std::mutex> lock{_mutex};
        std::cout << "waitForUserInterrupt() got lock" << '\n';
        _condition.wait(lock);
        std::cout << "user has signaled to interrupt program..." << '\n';
        lock.unlock();
    }
};
}// namespace NES