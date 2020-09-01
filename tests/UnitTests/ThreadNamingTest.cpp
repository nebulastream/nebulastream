#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <cstring>
#include <unistd.h>
#ifdef _POSIX_THREADS
#define HAS_POSIX_THREAD
#include <pthread.h>
#else
#error "Unsupported architecture"
#endif

namespace NES {
class ThreadNamingTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ThreadNamingTest.log", NES::LOG_DEBUG);

        NES_INFO("ThreadNamingTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() {
        NES_INFO("ThreadNamingTest test class TearDownTestCase.");
    }
};

TEST_F(ThreadNamingTest, testThreadNaming) {
    char threadName[17];
    setThreadName("NES-%d", 0);
    pthread_getname_np(pthread_self(), threadName, sizeof(threadName));
    ASSERT_TRUE(std::strcmp(threadName, "NES-0") == 0);
}

}