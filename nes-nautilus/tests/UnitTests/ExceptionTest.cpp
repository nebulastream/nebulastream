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

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include <cpptrace/from_current.hpp>
#include <cpptrace/from_current_macros.hpp>
#include <gtest/gtest.h>
#include <nautilus/CompilationStatistics.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/exception.hpp>
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
thread_local uint32_t cleanupCalls = 0;
thread_local std::function<void(int32_t*)> nestedCompiledInvocation;

int32_t* throwingPointerProxy(int32_t)
{
    throw std::runtime_error("pointer proxy");
}

int32_t incrementPotentiallyThrowing(int32_t value)
{
    return value + 1;
}

bool returnBoolPotentiallyThrowing(bool value)
{
    return value;
}

void recordCleanup() noexcept
{
    ++cleanupCalls;
}

void callNestedCompiledFunction(int32_t* output)
{
    nestedCompiledInvocation(output);
}

struct StructResult
{
    int32_t value;

    ~StructResult() noexcept { ++cleanupCalls; }
};

void writeStructNoexcept(StructResult* result, int32_t value) noexcept
{
    result->value = value;
}

void writeStructOrThrow(StructResult*, int32_t)
{
    throw std::runtime_error("struct proxy");
}

class TracedCleanup
{
public:
    ~TracedCleanup() { nautilus::invoke(recordCleanup); }
};

/// NautilusFunction records a named generated function and a generated CALL to it; it is not flattened into its
/// caller. This exercises exception propagation between functions in the same generated module.
nautilus::NautilusFunction throwingInnerGeneratedFunction{
    "throwing_inner",
    [](nautilus::val<int32_t*> output)
    {
        const TracedCleanup cleanup;
        auto pointer = nautilus::invoke(throwingPointerProxy, nautilus::val<int32_t>{1});
        const nautilus::val<int32_t> loaded = *pointer;
        *output = loaded;
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
        return nautilus::engine::NautilusEngine{options};
    }
};

/// Potentially throwing invokes in one generated function share a single lazily emitted stack holder. The trace-time
/// TLS cache contains only the symbolic val for this ALLOCA; the guarded proxy receives the generated stack pointer
/// and fills it with the executing thread's parked-exception address.
TEST_P(InvokeExceptionTest, GuardedInvokesReuseOneTraceLocalExceptionHolder)
{
    auto engine = makeEngine(GetParam(), true);
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            auto first = nautilus::invoke(incrementPotentiallyThrowing, nautilus::val<int32_t>{40});
            auto second = nautilus::invoke(incrementPotentiallyThrowing, first);
            *output = second;
        }));

    int32_t output = -1;
    EXPECT_NO_THROW(function(std::addressof(output)));
    EXPECT_EQ(output, 42);

    const auto statistics = function.getStatistics();
    ASSERT_NE(statistics, nullptr);
    const auto* compilationId = statistics->find("compilation.unitId");
    ASSERT_NE(compilationId, nullptr);
    const auto* id = std::get_if<std::string>(compilationId);
    ASSERT_NE(id, nullptr);
    const auto tracePath = std::filesystem::temp_directory_path() / "dump" / *id / "after_tracing.trace";
    std::ifstream traceFile{tracePath};
    ASSERT_TRUE(traceFile.is_open());
    const std::string trace{std::istreambuf_iterator<char>{traceFile}, std::istreambuf_iterator<char>{}};

    std::size_t allocaCount = 0;
    for (std::size_t position = 0; (position = trace.find("ALLOCA", position)) != std::string::npos; ++position)
    {
        ++allocaCount;
    }
    EXPECT_EQ(allocaCount, 1) << trace;
}

/// The trace-exit return for a non-void generated function must carry the same type as its ordinary return. In
/// particular, val<bool>'s default constructor records an i32 zero, so the fallback must be constructed explicitly
/// from bool rather than default-constructed.
TEST_P(InvokeExceptionTest, GuardedInvokeInBoolReturningFunctionUsesTypedTraceExitValue)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<nautilus::val<bool>(nautilus::val<bool>)>(
        [](nautilus::val<bool> value) { return nautilus::invoke(returnBoolPotentiallyThrowing, value); }));

    EXPECT_TRUE(function(true));
    EXPECT_FALSE(function(false));
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// Compilation and execution may happen on different threads. The generated trace passes its stack holder to the
/// proxy, which writes the executing thread's TLS address at runtime; it must not capture the tracing thread's TLS.
TEST_P(InvokeExceptionTest, GuardedInvokeUsesExecutingThreadsExceptionState)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            auto pointer = nautilus::invoke(throwingPointerProxy, nautilus::val<int32_t>{1});
            const nautilus::val<int32_t> value = *pointer;
            *output = value;
        }));

    int32_t output = -1;
    std::exception_ptr workerException;
    bool workerHasParkedException = true;
    std::thread worker(
        [&]
        {
            cpptrace::try_catch([&] { function(std::addressof(output)); }, [&] { workerException = std::current_exception(); });
            workerHasParkedException = nautilus::hasParkedException();
        });
    worker.join();

    EXPECT_NE(workerException, nullptr);
    EXPECT_FALSE(workerHasParkedException);
    EXPECT_EQ(output, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// A throwing pointer proxy used to return a nullptr sentinel and the following dereference crashed before the
/// parked exception reached ModuleFunction. The generated failure edge must unwind the trace-building C++ stack,
/// emit TracedCleanup's destructor, return from the compiled function, and only then rethrow in native C++.
TEST_P(InvokeExceptionTest, ThrowingInvokeExitsTraceBeforeUsingSentinel)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            const TracedCleanup cleanup;
            auto pointer = nautilus::invoke(throwingPointerProxy, nautilus::val<int32_t>{1});
            const nautilus::val<int32_t> loaded = *pointer;
            *output = loaded;
        }));

    cleanupCalls = 0;
    int32_t output = -1;
    EXPECT_THROW(function(std::addressof(output)), std::runtime_error);
    EXPECT_EQ(cleanupCalls, 1);
    EXPECT_EQ(output, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// Attributes supplied for the wrapped function cannot be copied to the guarded proxy call. In particular, the
/// wrapped function may only read memory, but the proxy always writes its hidden exception holder. Marking the proxy
/// as Ref lets the backend fold the post-call holder load to its initial null value and use the sentinel result.
TEST_P(InvokeExceptionTest, ThrowingInvokeWithRefAttributeExitsTraceBeforeUsingSentinel)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            const TracedCleanup cleanup;
            auto pointer = nautilus::invoke({.modRefInfo = nautilus::ModRefInfo::Ref}, throwingPointerProxy, nautilus::val<int32_t>{1});
            const nautilus::val<int32_t> loaded = *pointer;
            *output = loaded;
        }));

    cleanupCalls = 0;
    int32_t output = -1;
    EXPECT_THROW(function(std::addressof(output)), std::runtime_error);
    EXPECT_EQ(cleanupCalls, 1);
    EXPECT_EQ(output, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// A NautilusFunction produces a separate function in the generated module. Its failure return must be followed by a
/// TLS check in the generated caller so the caller also emits cleanup and returns instead of continuing ordinary work.
TEST_P(InvokeExceptionTest, ThrowingInvokePropagatesAcrossGeneratedFunctionCall)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            const TracedCleanup cleanup;
            throwingInnerGeneratedFunction(output);
            *output = 42;
        }));

    cleanupCalls = 0;
    int32_t output = -1;
    EXPECT_THROW(function(std::addressof(output)), std::runtime_error);
    EXPECT_EQ(cleanupCalls, 2);
    EXPECT_EQ(output, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// This is a genuinely nested compiled invocation rather than a generated call within one module: the outer JIT
/// function invokes native code, which enters a separately compiled ModuleFunction. The inner ModuleFunction must
/// leave the exception parked because an outer generated frame is still active. The outer generated function then
/// observes the TLS state, returns through its cleanup edge, and the outermost ModuleFunction rethrows.
TEST_P(InvokeExceptionTest, ThrowingInvokePropagatesAcrossNestedCompiledInvocation)
{
    auto engine = makeEngine(GetParam());
    auto innerFunction = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            const TracedCleanup cleanup;
            auto pointer = nautilus::invoke(throwingPointerProxy, nautilus::val<int32_t>{1});
            const nautilus::val<int32_t> loaded = *pointer;
            *output = loaded;
        }));

    nestedCompiledInvocation = [&innerFunction](int32_t* output) { innerFunction(output); };
    auto outerFunction = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            const TracedCleanup cleanup;
            nautilus::invoke(callNestedCompiledFunction, output);
            *output = 42;
        }));

    cleanupCalls = 0;
    int32_t output = -1;
    EXPECT_THROW(outerFunction(std::addressof(output)), std::runtime_error);
    EXPECT_EQ(cleanupCalls, 2);
    EXPECT_EQ(output, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
    nestedCompiledInvocation = {};
}

/// A noexcept function pointer keeps the original direct-call trace. In particular, no guarded proxy or parked-
/// exception branch is introduced around writeStructNoexcept. The val<StructResult> destructor is still emitted as
/// an ordinary noexcept cleanup call.
TEST_P(InvokeExceptionTest, NoexceptInvokeWithValStructUsesDirectCall)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            nautilus::val<StructResult> result;
            nautilus::invoke(writeStructNoexcept, &result, nautilus::val<int32_t>{42});
            const nautilus::val<int32_t> value = result.get(&StructResult::value);
            *output = value;
        }));

    cleanupCalls = 0;
    int32_t output = -1;
    EXPECT_NO_THROW(function(std::addressof(output)));
    EXPECT_EQ(output, 42);
    EXPECT_EQ(cleanupCalls, 1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// A potentially throwing function pointer is called through the guarded proxy. Its exception must make the traced
/// function return before reading the StructResult, while C++ unwinding still emits and executes the
/// val<StructResult> destructor.
TEST_P(InvokeExceptionTest, ThrowingInvokeWithValStructUnwindsBeforeReadingResult)
{
    auto engine = makeEngine(GetParam());
    auto function = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> output)
        {
            nautilus::val<StructResult> result;
            nautilus::invoke(writeStructOrThrow, &result, nautilus::val<int32_t>{42});
            const nautilus::val<int32_t> value = result.get(&StructResult::value);
            *output = value;
        }));

    cleanupCalls = 0;
    int32_t output = -1;
    EXPECT_THROW(function(std::addressof(output)), std::runtime_error);
    EXPECT_EQ(output, -1);
    EXPECT_EQ(cleanupCalls, 1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

INSTANTIATE_TEST_SUITE_P(
    TraceModes, InvokeExceptionTest, testing::Values(std::string("exceptionBasedTracing"), std::string("lazyTracing")));
}
}
