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

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <nautilus/nautilus_function.hpp>
#include <function.hpp>
#include <options.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>
#include <val_std.hpp>

namespace NES
{
namespace
{
thread_local uint32_t destructorCalls = 0;
thread_local std::array<int32_t, 4> destructorValues{};

struct ExceptionResult
{
    int32_t value = 0;

    ~ExceptionResult() noexcept { destructorValues.at(destructorCalls++) = value; }
};

void writeResult(ExceptionResult* result, int32_t value) noexcept
{
    result->value = value;
}

void throwWhileWriting(ExceptionResult*, int32_t)
{
    throw std::runtime_error("invoke failed");
}

void throwWithoutCleanup()
{
    throw std::runtime_error("invoke failed without cleanup");
}

nautilus::NautilusFunction nestedThrowingFunction{
    "nested_throwing_function",
    []
    {
        nautilus::val<ExceptionResult> inner;
        nautilus::invoke(writeResult, &inner, nautilus::val<int32_t>{2});
        nautilus::invoke(throwWhileWriting, &inner, nautilus::val<int32_t>{42});
    }};

class InvokeExceptionTest : public testing::TestWithParam<std::string>
{
protected:
    static nautilus::engine::NautilusEngine makeEngine(const std::string& traceMode, bool dumpAfterTracing = false)
    {
        nautilus::engine::Options options;
        options.setOption("engine.Compilation", true);
        options.setOption("engine.backend", std::string("mlir"));
        options.setOption("engine.compilationStrategy", std::string("legacy"));
        options.setOption("engine.traceMode", traceMode);
        options.setOption("mlir.enableMultithreading", false);
        options.setOption("dump.after_tracing", dumpAfterTracing);
        options.setOption("dump.after_mlir_generation", dumpAfterTracing);
        return nautilus::engine::NautilusEngine{options};
    }

    template <bool ThrowsAtInnerLevel, bool ThrowsAtMiddleLevel, bool ThrowsAtOuterLevel>
    static void runThreeLevelPermutation(const std::string& traceMode)
    {
        constexpr uint8_t throwingLevels = static_cast<uint8_t>(ThrowsAtInnerLevel) | static_cast<uint8_t>(ThrowsAtMiddleLevel) << 1U
            | static_cast<uint8_t>(ThrowsAtOuterLevel) << 2U;
        SCOPED_TRACE(testing::Message() << "throwing levels mask: " << static_cast<uint32_t>(throwingLevels));

        nautilus::NautilusFunction inner{
            "three_level_inner_" + std::to_string(throwingLevels),
            []() noexcept(!ThrowsAtInnerLevel)
            {
                nautilus::val<ExceptionResult> value;
                nautilus::invoke(writeResult, &value, nautilus::val<int32_t>{3});
                if constexpr (ThrowsAtInnerLevel)
                {
                    nautilus::invoke(throwWhileWriting, &value, nautilus::val<int32_t>{42});
                }
            }};
        nautilus::NautilusFunction middle{
            "three_level_middle_" + std::to_string(throwingLevels),
            [&inner]() noexcept(!(ThrowsAtInnerLevel || ThrowsAtMiddleLevel))
            {
                nautilus::val<ExceptionResult> value;
                nautilus::invoke(writeResult, &value, nautilus::val<int32_t>{2});
                inner();
                if constexpr (ThrowsAtMiddleLevel)
                {
                    nautilus::invoke(throwWhileWriting, &value, nautilus::val<int32_t>{42});
                }
            }};
        nautilus::NautilusFunction outer{
            "three_level_outer_" + std::to_string(throwingLevels),
            [&middle]() noexcept(!(ThrowsAtInnerLevel || ThrowsAtMiddleLevel || ThrowsAtOuterLevel))
            {
                nautilus::val<ExceptionResult> value;
                nautilus::invoke(writeResult, &value, nautilus::val<int32_t>{1});
                middle();
                if constexpr (ThrowsAtOuterLevel)
                {
                    nautilus::invoke(throwWhileWriting, &value, nautilus::val<int32_t>{42});
                }
            }};

        auto engine = makeEngine(traceMode, true);
        auto function = engine.registerFunction(std::function<void()>([&outer] { outer(); }));

        destructorCalls = 0;
        destructorValues.fill(0);
        if constexpr (ThrowsAtInnerLevel || ThrowsAtMiddleLevel || ThrowsAtOuterLevel)
        {
            EXPECT_THROW(function(), std::runtime_error);
        }
        else
        {
            EXPECT_NO_THROW(function());
        }

        ASSERT_EQ(destructorCalls, 3);
        EXPECT_EQ(destructorValues.at(0), 3);
        EXPECT_EQ(destructorValues.at(1), 2);
        EXPECT_EQ(destructorValues.at(2), 1);
    }

    template <typename Function>
    static std::string readTrace(const Function& function)
    {
        const auto statistics = function.getStatistics();
        EXPECT_NE(statistics, nullptr);
        if (statistics == nullptr)
        {
            return {};
        }

        const auto* compilationId = statistics->find("compilation.unitId");
        EXPECT_NE(compilationId, nullptr);
        if (compilationId == nullptr)
        {
            return {};
        }

        const auto* id = std::get_if<std::string>(compilationId);
        EXPECT_NE(id, nullptr);
        if (id == nullptr)
        {
            return {};
        }

        const auto tracePath = std::filesystem::temp_directory_path() / "dump" / *id / "after_tracing.trace";
        std::ifstream traceFile{tracePath};
        EXPECT_TRUE(traceFile.is_open());
        return {std::istreambuf_iterator<char>{traceFile}, std::istreambuf_iterator<char>{}};
    }
};

TEST_P(InvokeExceptionTest, PotentiallyThrowingInvokeRecordsCleanupMetadata)
{
    auto engine = makeEngine(GetParam(), true);
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            nautilus::val<ExceptionResult> result;
            nautilus::invoke(throwWhileWriting, &result, nautilus::val<int32_t>{42});
            const nautilus::val<int32_t> value = result.get(&ExceptionResult::value);
            *output = value;
        }));

    const auto trace = readTrace(function);
    EXPECT_NE(trace.find("CALL_WITH_EXCEPTION_HANDLING"), std::string::npos) << trace;
    EXPECT_NE(trace.find("unwind["), std::string::npos) << trace;
}

TEST_P(InvokeExceptionTest, ThrowingInvokeUnwindsLiveValStruct)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            nautilus::val<ExceptionResult> result;
            nautilus::invoke(throwWhileWriting, &result, nautilus::val<int32_t>{42});
            const nautilus::val<int32_t> value = result.get(&ExceptionResult::value);
            *output = value;
        }));

    destructorCalls = 0;
    destructorValues.fill(0);
    int32_t output = -1;
    EXPECT_THROW(function(std::addressof(output)), std::runtime_error);
    EXPECT_EQ(output, -1);
    EXPECT_EQ(destructorCalls, 1);
}

TEST_P(InvokeExceptionTest, MoveAssignmentDoesNotLeaveStaleCleanup)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void()>(
        []
        {
            {
                nautilus::val<ExceptionResult> destination;
                nautilus::invoke(writeResult, &destination, nautilus::val<int32_t>{1});
                nautilus::val<ExceptionResult> source;
                nautilus::invoke(writeResult, &source, nautilus::val<int32_t>{2});
                destination = std::move(source);
            }
            nautilus::invoke(throwWithoutCleanup);
        }));

    destructorCalls = 0;
    destructorValues.fill(0);
    EXPECT_THROW(function(), std::runtime_error);
    EXPECT_EQ(destructorCalls, 2);
    EXPECT_EQ(destructorValues.at(0), 1);
    EXPECT_EQ(destructorValues.at(1), 2);
    EXPECT_EQ(destructorValues.at(2), 0);
}

TEST_P(InvokeExceptionTest, ExceptionalCleanupsRunInReverseConstructionOrder)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void()>(
        []
        {
            nautilus::val<ExceptionResult> first;
            nautilus::invoke(writeResult, &first, nautilus::val<int32_t>{1});
            nautilus::val<ExceptionResult> second;
            nautilus::invoke(writeResult, &second, nautilus::val<int32_t>{2});
            nautilus::invoke(throwWhileWriting, &second, nautilus::val<int32_t>{42});
        }));

    destructorCalls = 0;
    destructorValues.fill(0);
    EXPECT_THROW(function(), std::runtime_error);
    ASSERT_EQ(destructorCalls, 2);
    EXPECT_EQ(destructorValues.at(0), 2);
    EXPECT_EQ(destructorValues.at(1), 1);
}

TEST_P(InvokeExceptionTest, ExceptionFromNestedNautilusFunctionUnwindsBothFrames)
{
    auto engine = makeEngine(GetParam(), true);
    auto function = engine.registerFunction(std::function<void()>(
        []
        {
            nautilus::val<ExceptionResult> outer;
            nautilus::invoke(writeResult, &outer, nautilus::val<int32_t>{1});
            nestedThrowingFunction();
        }));

    destructorCalls = 0;
    destructorValues.fill(0);
    EXPECT_THROW(function(), std::runtime_error);
    ASSERT_EQ(destructorCalls, 2);
    EXPECT_EQ(destructorValues.at(0), 2);
    EXPECT_EQ(destructorValues.at(1), 1);
}

TEST_P(InvokeExceptionTest, ThreeNestedFunctionsCoverEveryThrowingPermutation)
{
    runThreeLevelPermutation<false, false, false>(GetParam());
    runThreeLevelPermutation<true, false, false>(GetParam());
    runThreeLevelPermutation<false, true, false>(GetParam());
    runThreeLevelPermutation<true, true, false>(GetParam());
    runThreeLevelPermutation<false, false, true>(GetParam());
    runThreeLevelPermutation<true, false, true>(GetParam());
    runThreeLevelPermutation<false, true, true>(GetParam());
    runThreeLevelPermutation<true, true, true>(GetParam());
}

TEST_P(InvokeExceptionTest, ThrowingInvokeWithoutLiveCleanupRethrows)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            nautilus::invoke(throwWithoutCleanup);
            *output = 42;
        }));

    int32_t output = -1;
    EXPECT_THROW(function(std::addressof(output)), std::runtime_error);
    EXPECT_EQ(output, -1);
}

TEST_P(InvokeExceptionTest, NoexceptInvokeKeepsDirectCallPath)
{
    auto engine = makeEngine(GetParam(), true);
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            nautilus::val<ExceptionResult> result;
            nautilus::invoke(writeResult, &result, nautilus::val<int32_t>{42});
            const nautilus::val<int32_t> value = result.get(&ExceptionResult::value);
            *output = value;
        }));

    destructorCalls = 0;
    destructorValues.fill(0);
    int32_t output = -1;
    EXPECT_NO_THROW(function(std::addressof(output)));
    EXPECT_EQ(output, 42);
    EXPECT_EQ(destructorCalls, 1);

    const auto trace = readTrace(function);
    EXPECT_EQ(trace.find("CALL_WITH_EXCEPTION_HANDLING"), std::string::npos) << trace;
}

INSTANTIATE_TEST_SUITE_P(
    TraceModes, InvokeExceptionTest, testing::Values(std::string("exceptionBasedTracing"), std::string("lazyTracing")));
}
}
