#include <BorrowedPort.hpp>
#include <memory>
#include <Util/Logger/Logger.hpp>
#include <detail/PortDispatcher.hpp>

namespace NES::Testing {

using BorrowedPortPtr = std::shared_ptr<BorrowedPort>;

BorrowedPort::BorrowedPort(uint16_t port, uint32_t portIndex, detail::PortDispatcher* parent)
    : port(port), portIndex(portIndex), parent(parent) {}

BorrowedPort::~BorrowedPort() noexcept { parent->recyclePort(portIndex); }

BorrowedPort::operator uint16_t() const { return port; }

}// namespace NES::Testing