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

#include <Runtime/Profiler/PAPIProfiler.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <strstream>

using namespace std;
namespace NES {

class PapiProfilerTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("PapiProfilerTest.log", NES::LOG_DEBUG);
        NES_INFO("SetUpTestCase PapiProfilerTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("TearDownTestCase PapiProfilerTest test case."); }
};

TEST_F(PapiProfilerTest, testSampling) {
    using namespace Runtime::Profiler;
    std::ofstream buffer("/dev/null");
    Runtime::Profiler::PapiCpuProfiler profiler(PapiCpuProfiler::Presets::CachePresets, std::move(buffer), 0, 0);
    auto startTs = profiler.startSampling();
    sleep(1);
    auto stopTs = profiler.stopSampling(1);
    ASSERT_TRUE(startTs > 0 && stopTs > 0);
}
}// namespace NES