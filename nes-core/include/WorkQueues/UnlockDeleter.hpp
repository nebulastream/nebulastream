#ifndef NES_UNLOCKDELETER_HPP
#define NES_UNLOCKDELETER_HPP
#include <mutex>
namespace NES {

//idea taken from:
//https://stackoverflow.com/questions/23610561/return-locked-resource-from-class-with-automatic-unlocking#comment36245473_23610561
class UnlockDeleter {
  public:
    explicit UnlockDeleter();

    explicit UnlockDeleter(std::mutex mutex);

    explicit UnlockDeleter(std::mutex mutex, std::try_to_lock_t adoptLock);

    template <typename T>
    void operator () (T*) const noexcept {
        // no-op
    }

  private:
    std::unique_lock<std::mutex> lock;
};
}
#endif//NES_UNLOCKDELETER_HPP
