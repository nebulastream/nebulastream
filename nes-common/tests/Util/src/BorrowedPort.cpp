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
#include <BorrowedPort.hpp>
#include <Util/Logger/Logger.hpp>
#include <detail/PortDispatcher.hpp>
#include <memory>

namespace NES::Testing {

using BorrowedPortPtr = std::shared_ptr<BorrowedPort>;

BorrowedPort::BorrowedPort(uint16_t port, uint32_t portIndex, detail::PortDispatcher* parent)
    : port(port), portIndex(portIndex), parent(parent) {}

BorrowedPort::~BorrowedPort() noexcept { parent->recyclePort(portIndex); }

BorrowedPort::operator uint16_t() const { return port; }

}// namespace NES::Testing
