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

#ifndef NES_COMMON_TESTS_UTIL_INCLUDE_DETAIL_PORTDISPATCHER_HPP_
#define NES_COMMON_TESTS_UTIL_INCLUDE_DETAIL_PORTDISPATCHER_HPP_
#include <Util/FileMutex.hpp>
#include <atomic>
#include <detail/SharedMemoryFixedVector.hpp>
#include <memory>
namespace NES::Testing {
class BorrowedPort;
using BorrowedPortPtr = std::shared_ptr<BorrowedPort>;
namespace detail {

class PortDispatcher {
  private:
    static constexpr uint32_t CHECKSUM = 0x30011991;
    struct PortHolder {
        std::atomic<uint16_t> port;
        std::atomic<bool> free;
        std::atomic<uint32_t> checksum;
    };

  public:
    explicit PortDispatcher(uint16_t startPort, uint32_t numberOfPorts);

    ~PortDispatcher();

    void tearDown();

    BorrowedPortPtr getNextPort();

    void recyclePort(uint32_t portIndex);

  private:
    Util::FileMutex mutex;
    detail::SharedMemoryFixedVector<PortHolder> data;
    std::atomic<size_t> numOfBorrowedPorts{0};
};

PortDispatcher& getPortDispatcher();

}// namespace detail
}// namespace NES::Testing

#endif // NES_COMMON_TESTS_UTIL_INCLUDE_DETAIL_PORTDISPATCHER_HPP_
