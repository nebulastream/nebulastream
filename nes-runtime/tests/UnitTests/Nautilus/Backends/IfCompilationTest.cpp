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

#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class IfCompilationTest : public Testing::NESBaseTest, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("IfCompilationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup IfCompilationTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down IfCompilationTest test class."); }
};

Value<> ifThenCondition() {
    Value value = 1;
    Value iw = 1;
    if (value == 42) {
        iw = iw + 1;
    }
    return iw + 42;
}

TEST_P(IfCompilationTest, ifConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return ifThenCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int16_t (*)()>("execute");
    ASSERT_EQ(function(), 43);
}

Value<> ifThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
        iw = iw + 1;
    } else {
        iw = iw + 42;
    }
    return iw + 42;
}

TEST_P(IfCompilationTest, ifThenElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return ifThenElseCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 85);
}

Value<> nestedIfThenElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
    } else {
        if (iw == 8) {
        } else {
            iw = iw + 2;
        }
    }
    return iw = iw + 2;
}

TEST_P(IfCompilationTest, nestedIFThenElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return nestedIfThenElseCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 5);
}

Value<> nestedIfNoElseCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (value == 42) {
        iw = iw + 4;
    } else {
        iw = iw + 9;
        if (iw == 8) {
            iw + 14;
        }
    }
    return iw = iw + 2;
}

TEST_P(IfCompilationTest, nestedIFThenNoElse) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return nestedIfNoElseCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 12);
}

// Todo leads to redundant if that can be replaced with br
Value<> doubleIfCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (iw == 8) {
        // iw = iw + 14;
    }
    if (iw == 1) {
        iw = iw + 20;
    }
    return iw = iw + 2;
}

TEST_P(IfCompilationTest, doubleIfCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return doubleIfCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 23);
}

Value<> ifElseIfCondition() {
    Value value = Value(1);
    Value iw = Value(1);
    if (iw == 8) {
        iw = iw + 14;
    } else if (iw == 1) {
        iw = iw + 20;
    }
    return iw = iw + 2;
}

TEST_P(IfCompilationTest, ifElseIfCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return ifElseIfCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 23);
}

Value<> deeplyNestedIfElseCondition() {
    Value value = Value(1);
    Value iw = Value(5);
    if (iw < 8) {
        if (iw > 6) {
            iw = iw + 10;
        } else {
            if (iw < 6) {
                if (iw == 5) {
                    iw = iw + 5;
                }
            }
        }
    } else {
        iw = iw + 20;
    }
    return iw = iw + 2;
}

TEST_P(IfCompilationTest, deeplyNestedIfElseCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return deeplyNestedIfElseCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 12);
}

Value<> deeplyNestedIfElseIfCondition() {
    Value value = Value(1);
    Value iw = Value(5);
    if (iw < 8) {
        iw = iw + 10;
    } else {
        if (iw == 5) {
            iw = iw + 5;
        } else if (iw == 4) {
            iw = iw + 4;
        }
    }
    return iw = iw + 2;
}

TEST_P(IfCompilationTest, deeplyNestedIfElseIfCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([]() {
        return deeplyNestedIfElseIfCondition();
    });
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 17);
}

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        IfCompilationTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<IfCompilationTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus