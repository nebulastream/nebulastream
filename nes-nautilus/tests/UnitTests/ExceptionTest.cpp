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

/// What happens to an exception thrown by a proxy: that a guarded call surfaces it at the compiled-call boundary,
/// what such a call hands back in the meantime, and where that value is safe to use.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <DataTypes/DataTypesUtil.hpp>
#include <gtest/gtest.h>
#include <nautilus/exception.hpp>
#include <Engine.hpp>
#include <function.hpp>
#include <options.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
nautilus::engine::NautilusEngine makeCompiledEngine()
{
    nautilus::engine::EngineOptions options;
    options.setOption("engine.Compilation", true);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    options.setOption("engine.traceMode", std::string("legacy"));
    options.setOption("mlir.enableMultithreading", false);
    return nautilus::engine::NautilusEngine{options};
}

nautilus::engine::NautilusEngine makeInterpretedEngine()
{
    nautilus::engine::EngineOptions options;
    options.setOption("engine.Compilation", false);
    return nautilus::engine::NautilusEngine{options};
}

struct SlotResult
{
    int64_t value;
    bool isNull;
};

int32_t* throwingPointerProxy(int32_t)
{
    throw std::runtime_error("pointer proxy");
}

int32_t nonThrowingPointerTarget = 7;

/// Same shape as throwingPointerProxy, but on this run it does not throw.
int32_t* nonThrowingPointerProxy(int32_t)
{
    return std::addressof(nonThrowingPointerTarget);
}

SlotResult throwingAggregateProxy(int32_t)
{
    throw std::runtime_error("aggregate proxy");
}

int64_t throwingScalarProxy(int32_t)
{
    throw std::runtime_error("scalar proxy");
}

thread_local int64_t countingProxyCalls = 0;

int64_t countingProxy(int32_t)
{
    ++countingProxyCalls;
    return 42;
}

}

static void testProxy(int)
{
    throw std::runtime_error("test");
}

/// A captureless lambda that throws surfaces as an exception from the compiled call, leaving nothing parked.
TEST(InvokeGuardedTest, rethrowsThrowFromCapturelessLambda)
{
    auto engine = makeCompiledEngine();
    auto fn = engine.registerFunction(std::function<void()>(
        []()
        {
            const nautilus::val<int> x = 1;
            nautilus::invokeGuarded([](int) { throw std::runtime_error("lambda"); }, x);
        }));

    EXPECT_THROW(fn(), std::runtime_error);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// Same for a named function passed by value.
TEST(InvokeGuardedTest, rethrowsThrowFromNamedFunction)
{
    auto engine = makeCompiledEngine();
    auto fn = engine.registerFunction(std::function<void()>(
        []()
        {
            const nautilus::val<int> x = 1;
            nautilus::invokeGuarded(testProxy, x);
        }));

    EXPECT_THROW(fn(), std::runtime_error);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// In the interpreter the throw unwinds directly and nothing is parked.
TEST(InvokeGuardedTest, interpreterThrowsWithoutParking)
{
    auto engine = makeInterpretedEngine();
    auto fn = engine.registerFunction(std::function<void()>(
        []()
        {
            const nautilus::val<int> x = 1;
            nautilus::invokeGuarded(testProxy, x);
        }));

    EXPECT_THROW(fn(), std::runtime_error);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// Dereferencing a parked pointer without checking first is a crash -- the counterexample for the gated tests below.
TEST(InvokeGuardedTest, parkedPointerSentinelFaultsOnDereference)
{
    EXPECT_DEATH(
        {
            auto engine = makeCompiledEngine();
            auto fn = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
                [](nautilus::val<int32_t*> out)
                {
                    /// Not const: val<T*>::operator* is non-const.
                    auto ptr = nautilus::invoke(nautilus::details::guardedProxy<&throwingPointerProxy, int32_t>, nautilus::val<int32_t>{1});
                    const nautilus::val<int32_t> loaded = *ptr;
                    *out = loaded;
                }));
            int32_t out = 0;
            /// Marker so a death for any other reason fails the match.
            std::cerr << "compiled" << std::flush;
            fn(std::addressof(out));
        },
        "compiled");
}

/// Returning early once a proxy has parked turns that crash into an exception, and leaves the output untouched.
TEST(InvokeGuardedTest, guardedPointerEarlyReturnExitsCompiledFunction)
{
    auto engine = makeCompiledEngine();
    auto fn = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> out)
        {
            auto ptr = nautilus::invokeGuardedPtr<&throwingPointerProxy, int32_t>(nautilus::val<int32_t>{1});
            if (nautilus::hasParkedExceptionTraced())
            {
                return;
            }
            const nautilus::val<int32_t> loaded = *ptr;
            *out = loaded;
        }));

    int32_t out = -1;
    EXPECT_THROW(fn(std::addressof(out)), std::runtime_error);
    EXPECT_EQ(out, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// Skipping just the dereference, instead of returning, does the same.
TEST(InvokeGuardedTest, guardedPointerGateSkipsDereferenceAfterThrow)
{
    auto engine = makeCompiledEngine();
    auto fn = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> out)
        {
            auto ptr = nautilus::invokeGuardedPtr<&throwingPointerProxy, int32_t>(nautilus::val<int32_t>{1});
            if (not nautilus::hasParkedExceptionTraced())
            {
                const nautilus::val<int32_t> loaded = *ptr;
                *out = loaded;
            }
        }));

    int32_t out = -1;
    EXPECT_THROW(fn(std::addressof(out)), std::runtime_error);
    EXPECT_EQ(out, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// The gate must not fire when the proxy did not throw: the store still happens.
TEST(InvokeGuardedTest, guardedPointerGateDoesNotMisfireOnSuccessPath)
{
    auto engine = makeCompiledEngine();
    auto fn = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> out)
        {
            auto ptr = nautilus::invokeGuardedPtr<&nonThrowingPointerProxy, int32_t>(nautilus::val<int32_t>{1});
            if (not nautilus::hasParkedExceptionTraced())
            {
                const nautilus::val<int32_t> loaded = *ptr;
                *out = loaded;
            }
        }));

    int32_t out = -1;
    fn(std::addressof(out));
    EXPECT_EQ(out, nonThrowingPointerTarget);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// The same guarded pointer call throws directly in the interpreter, with nothing parked.
TEST(InvokeGuardedTest, interpreterGuardedPointerThrowsWithoutParking)
{
    auto engine = makeInterpretedEngine();
    auto fn = engine.registerFunction(std::function<void(nautilus::val<int32_t*>)>(
        [](nautilus::val<int32_t*> out)
        {
            auto ptr = nautilus::invokeGuardedPtr<&throwingPointerProxy, int32_t>(nautilus::val<int32_t>{1});
            if (nautilus::hasParkedExceptionTraced())
            {
                return;
            }
            const nautilus::val<int32_t> loaded = *ptr;
            *out = loaded;
        }));

    int32_t out = -1;
    EXPECT_THROW(fn(std::addressof(out)), std::runtime_error);
    EXPECT_EQ(out, -1);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// An aggregate proxy hands back a zeroed slot after a throw, so the field loads are safe.
TEST(InvokeGuardedTest, guardedSlotStaysDereferenceableAfterThrow)
{
    auto engine = makeCompiledEngine();
    auto fn = engine.registerFunction(std::function<void(nautilus::val<int64_t*>)>(
        [](nautilus::val<int64_t*> out)
        {
            const auto slot = nautilus::invokeGuarded<&throwingAggregateProxy, int32_t>(nautilus::val<int32_t>{1});
            const nautilus::val<int64_t> value = *getMemberWithOffset<int64_t>(slot, offsetof(SlotResult, value));
            *out = value;
        }));

    int64_t out = -1;
    EXPECT_THROW(fn(std::addressof(out)), std::runtime_error);
    EXPECT_EQ(out, 0);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// Once one proxy has parked, later guarded proxies do not run, but the traced code around them still does.
TEST(InvokeGuardedTest, parkedExceptionShortCircuitsLaterGuardedInvokes)
{
    auto engine = makeCompiledEngine();
    auto fn = engine.registerFunction(std::function<void(nautilus::val<int64_t*>)>(
        [](nautilus::val<int64_t*> out)
        {
            nautilus::invokeGuarded<&throwingScalarProxy, int32_t>(nautilus::val<int32_t>{1});
            *out = nautilus::invokeGuarded<&countingProxy, int32_t>(nautilus::val<int32_t>{1});
        }));

    countingProxyCalls = 0;
    int64_t out = -1;
    EXPECT_THROW(fn(std::addressof(out)), std::runtime_error);
    EXPECT_EQ(countingProxyCalls, 0);
    EXPECT_EQ(out, 0);
    EXPECT_FALSE(nautilus::hasParkedException());
}

/// A parked exception does not leak into the next invocation on the same thread.
TEST(InvokeGuardedTest, parkedExceptionDoesNotLeakIntoNextInvocation)
{
    auto engine = makeCompiledEngine();
    auto throwing = engine.registerFunction(
        std::function<void()>([]() { nautilus::invokeGuarded<&throwingScalarProxy, int32_t>(nautilus::val<int32_t>{1}); }));
    auto counting = engine.registerFunction(std::function<void(nautilus::val<int64_t*>)>(
        [](nautilus::val<int64_t*> out) { *out = nautilus::invokeGuarded<&countingProxy, int32_t>(nautilus::val<int32_t>{1}); }));

    EXPECT_THROW(throwing(), std::runtime_error);

    countingProxyCalls = 0;
    int64_t out = -1;
    counting(std::addressof(out));
    EXPECT_EQ(countingProxyCalls, 1);
    EXPECT_EQ(out, 42);
}

}
