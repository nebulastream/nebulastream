#include <TestWithErrorHandling.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
namespace Exceptions {
extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
extern void removeGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
}// namespace Exceptions

namespace Testing {

void TestWithErrorHandling::SetUp() {
    testing::Test::SetUp();
    Exceptions::installGlobalErrorListener(self = std::shared_ptr<Exceptions::ErrorListener>(this, Deleter()));
    startWaitingThread(typeid(*this).name());
}

void TestWithErrorHandling::TearDown() {
    testing::Test::TearDown();
    completeTest();
    Logger::getInstance()->forceFlush();
    Exceptions::removeGlobalErrorListener(self);
    self.reset();
}

void TestWithErrorHandling::onFatalError(int signalNumber, std::string callstack) {
    NES_ERROR("onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack);
    failTest();
    FAIL();
}

void TestWithErrorHandling::onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) {
    NES_ERROR("onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack);
    failTest();
    FAIL();
}

namespace detail {

TestWaitingHelper::TestWaitingHelper() { testCompletion = std::make_shared<std::promise<bool>>(); }

void TestWaitingHelper::failTest() {
    auto expected = false;
    if (testCompletionSet.compare_exchange_strong(expected, true)) {
        testCompletion->set_value(false);
        waitThread->join();
        waitThread.reset();
    }
}

void TestWaitingHelper::completeTest() {
    auto expected = false;
    if (testCompletionSet.compare_exchange_strong(expected, true)) {
        testCompletion->set_value(true);
        waitThread->join();
        waitThread.reset();
    }
}

void TestWaitingHelper::startWaitingThread(std::string testName) {
    auto self = this;
    waitThread = std::make_unique<std::thread>([this, testName = std::move(testName)]() mutable {
        auto future = testCompletion->get_future();
        switch (future.wait_for(std::chrono::minutes(WAIT_TIME_SETUP))) {
            case std::future_status::ready: {
                try {
                    auto res = future.get();
                    ASSERT_TRUE(res);
                    if (!res) {
                        NES_FATAL_ERROR2("Got error in test [{}]", testName);
                        std::exit(-127);
                    }
                } catch (std::exception const& exception) {
                    NES_FATAL_ERROR2("Got exception in test [{}]: {}", testName, exception.what());
                    FAIL();
                    std::exit(-1);
                }
                break;
            }
            case std::future_status::timeout:
            case std::future_status::deferred: {
                NES_ERROR2("Cannot terminate test [{}] within deadline", testName);
                FAIL();
                std::exit(-127);
                break;
            }
        }
    });
}
}


}// namespace Testing
}// namespace NES