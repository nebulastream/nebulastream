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

#include <BaseUnitTest.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Nautilus/Util/IRDumpHandler.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <cstdint>
#include <gtest/gtest-param-test.h>
#include <gtest/internal/gtest-param-util.h>
#include <memory>

using namespace NES::Nautilus;
namespace NES::Nautilus {

class LoopCompilationTest : public Testing::BaseUnitTest, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TraceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TraceTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down TraceTest test class."); }
};

Value<> sumLoop(int upperLimit) {
    for (Value start = 0; start < upperLimit; start = start + 1) {
    }
    return Value(0);
}

TEST_P(LoopCompilationTest, sumLoopTestSCF) {
    auto execution = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return sumLoop(10);
    });

    auto engine = AbstractCompilationBackendTest::prepareIr(execution);
    std::set<IR::OperationPtr> ptrSet;
    NES_DEBUG("{}", engine->toString());
    engine->getRootOperation()->detectCycle(ptrSet, engine->getRootOperation());
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
INSTANTIATE_TEST_CASE_P(testLoopCompilation,
                        LoopCompilationTest,
                        ::testing::Values("MLIR"),
                        [](const testing::TestParamInfo<LoopCompilationTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus