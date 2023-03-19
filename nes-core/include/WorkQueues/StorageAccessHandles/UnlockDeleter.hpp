/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_UNLOCKDELETER_HPP
#define NES_UNLOCKDELETER_HPP
#include <mutex>
namespace NES {

//idea taken from:
//https://stackoverflow.com/questions/23610561/return-locked-resource-from-class-with-automatic-unlocking#comment36245473_23610561
class UnlockDeleter {
  public:
    explicit UnlockDeleter();

    explicit UnlockDeleter(std::mutex& mutex);

    explicit UnlockDeleter(std::mutex& mutex, std::try_to_lock_t tryToLock);

    template <typename T>
    void operator () (T*) const noexcept {
        // no-op
    }

  private:
    std::unique_lock<std::mutex> lock;
};
}
#endif//NES_UNLOCKDELETER_HPP
