/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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