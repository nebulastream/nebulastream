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

#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/Backends/MLIR/MLIRCompilationBackend.hpp>
#include <Nautilus/Backends/MLIR/MLIRUtility.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

using namespace NES::Nautilus;
namespace NES::Nautilus {

class IfCompilationTest : public testing::Test {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("IfCompilationTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup IfCompilationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }

    auto prepare(std::shared_ptr<Nautilus::Tracing::ExecutionTrace> executionTrace) {
        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        std::cout << *executionTrace.get() << std::endl;
        auto ir = irCreationPhase.apply(executionTrace);
        std::cout << ir->toString() << std::endl;
        return Backends::MLIR::MLIRCompilationBackend().compile(ir);
    }
};

Value<> ifThenCondition() {
    Value value = 1;
    Value iw = 1;
    if (value == 42) {
        iw = iw + 1;
    }
    return iw + 42;
}

TEST_F(IfCompilationTest, ifConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return ifThenCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

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

TEST_F(IfCompilationTest, ifThenElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return ifThenElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

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

TEST_F(IfCompilationTest, nestedIFThenElseConditionTest) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return nestedIfThenElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

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

TEST_F(IfCompilationTest, nestedIFThenNoElse) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return nestedIfNoElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t (*)()>("execute");
    ASSERT_EQ(function(), 12);
}

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

TEST_F(IfCompilationTest, doubleIfCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return doubleIfCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t(*)()>("execute");
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

TEST_F(IfCompilationTest, ifElseIfCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return ifElseIfCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;

    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t(*)()>("execute");
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

TEST_F(IfCompilationTest, DISABLED_deeplyNestedIfElseCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return deeplyNestedIfElseCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);

    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t(*)()>("execute");
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

// Currently fails, because an empty block (Block 7) is created, during trace building.
TEST_F(IfCompilationTest, DISABLED_deeplyNestedIfElseIfCondition) {
    auto executionTrace = Nautilus::Tracing::traceFunctionSymbolicallyWithReturn([]() {
        return deeplyNestedIfElseIfCondition();
    });
    std::cout << *executionTrace.get() << std::endl;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    std::cout << *executionTrace.get() << std::endl;
    auto ir = irCreationPhase.apply(executionTrace);

    auto engine = prepare(executionTrace);
    auto function = engine->getInvocableMember<int32_t(*)()>("execute");
    ASSERT_EQ(function(), 12);
}

}// namespace NES::Nautilus