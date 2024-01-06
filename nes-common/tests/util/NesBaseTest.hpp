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
#ifndef NES_TESTS_UTIL_NESBASETEST_HPP_
#define NES_TESTS_UTIL_NESBASETEST_HPP_

#include <Exceptions/ErrorListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <future>
#include <gtest/gtest.h>
#include <thread>
#include <typeinfo>

#define ASSERT_INSTANCE_OF(node, instance)                                                                                       \
    if (!(node)->instanceOf<instance>()) {                                                                                       \
        auto message = (node)->toString() + " is not of instance " + std::string(typeid(instance).name());                       \
        GTEST_FATAL_FAILURE_(message.c_str());                                                                                   \
    }

namespace NES {
namespace Exceptions {
extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
extern void removeGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
}// namespace Exceptions
namespace Testing {
namespace detail {
class TestWaitingHelper {
  public:
    TestWaitingHelper();
    void startWaitingThread(std::string testName);
    void completeTest();
    void failTest();

  private:
//    std::unique_ptr<std::thread> waitThread;
    std::shared_ptr<std::promise<bool>> testCompletion;
    std::atomic<bool> testCompletionSet{false};
    static constexpr uint64_t WAIT_TIME_SETUP = 10000000;
};
}// namespace detail
template<typename T>
class TestWithErrorHandling : public T, public Exceptions::ErrorListener, public detail::TestWaitingHelper {
    struct Deleter {
        void operator()(void*) {}
    };

  public:
    void SetUp() override {
        T::SetUp();
        Exceptions::installGlobalErrorListener(self = std::shared_ptr<Exceptions::ErrorListener>(this, Deleter()));
//        startWaitingThread(typeid(*this).name());
    }

    void TearDown() override {
        T::TearDown();
        completeTest();
        Logger::getInstance()->forceFlush();
        Exceptions::removeGlobalErrorListener(self);
        self.reset();
    }

    virtual void onFatalError(int signalNumber, std::string callstack) override {
        NES_ERROR("onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack);
        failTest();
        FAIL();
    }

    virtual void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        NES_ERROR("onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack);
        failTest();
        FAIL();
    }

  private:
    std::shared_ptr<Exceptions::ErrorListener> self{nullptr};
};

class NesPortDispatcher;

/**
 * @brief A borrowed port for the port pool of nes test base class.
 * It manages garbage collection internally when dtor is called.
 */
class BorrowedPort {
  private:
    uint16_t port;
    uint32_t portIndex;
    NesPortDispatcher* parent;

  public:
    /**
     * @brief Creates a new port wrapper object
     * @param port the port value
     * @param portIndex the index of the port in the pool
     * @param parent the pool object
     */
    explicit BorrowedPort(uint16_t port, uint32_t portIndex, NesPortDispatcher* parent)
        : port(port), portIndex(portIndex), parent(parent) {}

    ~BorrowedPort() noexcept;

    [[nodiscard]] inline operator uint16_t() const { return port; }
};
using BorrowedPortPtr = std::shared_ptr<BorrowedPort>;

class NESBaseTest : public Testing::TestWithErrorHandling<testing::Test> {
    friend class BorrowedPort;
    using Base = TestWithErrorHandling<testing::Test>;

  protected:
    BorrowedPortPtr rpcCoordinatorPort{nullptr};
    BorrowedPortPtr restPort{nullptr};

  public:
    /**
     * @brief the base test class ctor that creates the internal test resources
     */
    explicit NESBaseTest();

    ~NESBaseTest();

    /**
     * @brief Fetches the port
     */
    void SetUp() override;

    /**
     * @brief Release internal ports
     */
    void TearDown() override;

    void onFatalError(int signalNumber, std::string callstack) override;

    void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override;

  protected:
    /**
     * @brief Retrieve another free port
     * @return a free port
     */
    BorrowedPortPtr getAvailablePort();

    /**
     * @brief returns the test resource folder to write files
     * @return the test folder
     */
    std::filesystem::path getTestResourceFolder() const;

  private:
    std::filesystem::path testResourcePath;

    std::atomic<bool> setUpCalled{false};
    std::atomic<bool> tearDownCalled{false};
};
}// namespace Testing

}// namespace NES

#endif//NES_TESTS_UTIL_NESBASETEST_HPP_