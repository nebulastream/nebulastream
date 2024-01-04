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
#include <filesystem>
#include <mutex>
#include <unistd.h>
#if defined(__linux__)
#include <pwd.h>
#endif

namespace NES::Testing::detail {

PortDispatcher& getPortDispatcher() {
    static PortDispatcher portDispatcher(8000, 10000);
    return portDispatcher;
}

// trim from start (in place)
static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
}

// trim from end (in place)
static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(),
                         s.rend(),
                         [](unsigned char ch) {
                             return !std::isspace(ch);
                         })
                .base(),
            s.end());
}

static inline std::string trim(std::string s) {
    std::string str(s);
    rtrim(str);
    ltrim(str);
    return str;
}

static inline std::string getUsername() {
#if defined(__linux__)
    uid_t uid = geteuid();
    struct passwd* pw = getpwuid(uid);
    if (pw) {
        return std::string(pw->pw_name);
    }
#endif
    return getenv("USER");
}

static inline auto getMutexPath() {
    using namespace std::string_literals;
    auto tmpPath = std::filesystem::temp_directory_path();
    auto fileName = detail::trim(getUsername()) + ".nes.lock"s;
    return tmpPath / fileName;
}

static inline auto getPortPoolName() {
    using namespace std::string_literals;
    auto username = detail::trim(getUsername());
    return "/"s + username + ".nes.port.pool"s;
}

PortDispatcher::PortDispatcher(uint16_t startPort, uint32_t numberOfPorts)
    : mutex(detail::getMutexPath()), data(detail::getPortPoolName(), numberOfPorts) {
    std::unique_lock<Util::FileMutex> lock(mutex, std::defer_lock);
    if (lock.try_lock()) {
        data.open();
        if (data.isCreated()) {
            for (uint16_t current = startPort, i = 0; i < numberOfPorts; ++i, ++current) {
                data[i].port = current;
                data[i].free = true;
                data[i].checksum = CHECKSUM;
            }
        }
    } else {
        lock.lock();
        data.open();
    }
}

BorrowedPortPtr PortDispatcher::getNextPort() {
    uint64_t tryCount = 0;
    //We try 100 times
    while (++tryCount < 100) {
        auto nextIndex = data.getNextIndex();
        auto expected = true;
        if (data[nextIndex].checksum != CHECKSUM) {
            std::cerr << "invalid checksum " << data[nextIndex].checksum << "!=" << CHECKSUM << std::endl;
        }
        NES_ASSERT2_FMT(data[nextIndex].checksum == CHECKSUM, "invalid checksum");
        if (data[nextIndex].free.compare_exchange_strong(expected, false)) {
            ++numOfBorrowedPorts;
            return std::make_shared<BorrowedPort>(data[nextIndex].port, nextIndex, this);
        }
        usleep(100 * 1000);// 100ms
    }
    std::cerr << "Cannot find free port" << std::endl;
    NES_ASSERT(false, "Cannot find free port");
    return nullptr;
}

void PortDispatcher::recyclePort(uint32_t portIndex) {
    auto expected = false;
    if (data[portIndex].free.compare_exchange_strong(expected, true)) {
        --numOfBorrowedPorts;
    } else {
        NES_ASSERT2_FMT(false, "Port " << data[portIndex].port << " recycled twice");
    }
}

void PortDispatcher::tearDown() {
    NES_ASSERT2_FMT(numOfBorrowedPorts == 0,
                    "There are " << numOfBorrowedPorts << " leaked ports - please check your test logic");
}

PortDispatcher::~PortDispatcher() { tearDown(); }

}// namespace NES::Testing::detail