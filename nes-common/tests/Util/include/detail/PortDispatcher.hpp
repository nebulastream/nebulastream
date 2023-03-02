//
// Created by pgrulich on 02.03.23.
//

#ifndef NES_NES_COMMON_TESTS_UTIL_SRC_PORTDISPATCHER_HPP_
#define NES_NES_COMMON_TESTS_UTIL_SRC_PORTDISPATCHER_HPP_
#include <Util/FileMutex.hpp>
#include <atomic>
#include <detail/ShmFixedVector.hpp>
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
    detail::ShmFixedVector<PortHolder> data;
    std::atomic<size_t> numOfBorrowedPorts{0};
};

PortDispatcher& getPortDispatcher();

}// namespace detail
}// namespace NES::Testing

#endif//NES_NES_COMMON_TESTS_UTIL_SRC_PORTDISPATCHER_HPP_
