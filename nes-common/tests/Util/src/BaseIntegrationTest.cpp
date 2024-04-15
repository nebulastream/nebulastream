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

#include <BaseIntegrationTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <detail/PortDispatcher.hpp>
#include <filesystem>
#include <mutex>
#include <random>
#if defined(__linux__)
#endif
namespace NES::Testing {
namespace detail::uuid {
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);
static std::uniform_int_distribution<> dis2(8, 11);

std::string generateUUID() {
    std::stringstream ss;
    int i;
    ss << std::hex;
    for (i = 0; i < 8; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 4; i++) {
        ss << dis(gen);
    }
    ss << "-4";
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 12; i++) {
        ss << dis(gen);
    }
    return ss.str();
}
}// namespace detail::uuid

BaseIntegrationTest::BaseIntegrationTest() : testResourcePath(std::filesystem::current_path() / detail::uuid::generateUUID()) {}

void BaseIntegrationTest::SetUp() {
    auto expected = false;
    if (setUpCalled.compare_exchange_strong(expected, true)) {
        NES::Testing::BaseUnitTest::SetUp();
        if (!std::filesystem::exists(testResourcePath)) {
            std::filesystem::create_directories(testResourcePath);
        } else {
            std::filesystem::remove_all(testResourcePath);
            std::filesystem::create_directories(testResourcePath);
        }
        restPort = detail::getPortDispatcher().getNextPort();
        rpcCoordinatorPort = detail::getPortDispatcher().getNextPort();
    } else {
        NES_ERROR("SetUp called twice in {}", typeid(*this).name());
    }
}

BorrowedPortPtr BaseIntegrationTest::getAvailablePort() { return detail::getPortDispatcher().getNextPort(); }

std::filesystem::path BaseIntegrationTest::getTestResourceFolder() const { return testResourcePath; }

BaseIntegrationTest::~BaseIntegrationTest() {
    NES_ASSERT2_FMT(setUpCalled, "SetUp not called for test " << typeid(*this).name());
    NES_ASSERT2_FMT(tearDownCalled, "TearDown not called for test " << typeid(*this).name());
}

void BaseIntegrationTest::TearDown() {
    auto expected = false;
    if (tearDownCalled.compare_exchange_strong(expected, true)) {
        restPort.reset();
        rpcCoordinatorPort.reset();
        std::filesystem::remove_all(testResourcePath);
        NES::Testing::BaseUnitTest::TearDown();
        completeTest();
    } else {
        NES_ERROR("TearDown called twice in {}", typeid(*this).name());
    }
}

void BaseIntegrationTest::onFatalError(int signalNumber, std::string callstack) {
    if (callstack.empty()) {
        NES_ERROR("onFatalError: signal [{}] error [{}]", signalNumber, strerror(errno));
    } else {
        NES_ERROR("onFatalError: signal [{}] error [{}] callstack: {}", signalNumber, strerror(errno), callstack);
    }
    failTest();
}

void BaseIntegrationTest::onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) {
    if (callstack.empty()) {
        NES_ERROR("onFatalException: exception=[{}] callstack={}", exception->what(), callstack);
    } else {
        NES_ERROR("onFatalException: exception=[{}] callstack={}", exception->what(), callstack);
    }
    failTest();
}

}// namespace NES::Testing