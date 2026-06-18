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

#include <Interface/NautilusBuffer.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <BaseUnitTest.hpp>
#include <function.hpp>
#include <options.hpp>
#include <val_arith.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// Suppressed intentionally for these tests: the small record counts are illustrative magic numbers, the nautilus::val arguments are
/// passed by value (cheap handles) to match the traced-function signature, and the copy-construction test deliberately copies a buffer.
/// NOLINTBEGIN(readability-magic-numbers, performance-unnecessary-value-param, performance-unnecessary-copy-initialization)
constexpr size_t TEST_BUFFER_SIZE = 4096;
constexpr size_t TEST_NUMBER_OF_BUFFERS = 16;
/// Number of records pre-set on the on-stack TupleBuffer, so that reads through a BorrowedNautilusBuffer can be verified.
constexpr uint64_t ON_STACK_RECORD_COUNT = 11;

enum class EngineMode : std::uint8_t
{
    Interpreter,
    Compiler
};

nautilus::engine::NautilusEngine makeEngine(EngineMode mode)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", mode == EngineMode::Compiler);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    options.setOption("mlir.enableMultithreading", false);
    return nautilus::engine::NautilusEngine{options};
}

/// Fixture parameterized by the nautilus backend, so every test runs once for the interpreter and once for the compiler.
class NautilusBufferTest : public Testing::BaseUnitTest, public testing::WithParamInterface<EngineMode>
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("NautilusBufferTest.log", LogLevel::LOG_DEBUG); }

protected:
    /// Compiles and runs `body` in the backend selected by the test parameter. The body receives a pointer to a TupleBuffer that lives
    /// on the (C++) stack of this function - mimicking the WorkerThread stack that owns the entry buffer of a pipeline - and a buffer
    /// provider it can allocate owned buffers from. Lifetime correctness is verified implicitly: BufferManager::destroy() asserts that
    /// every buffer handed out has been returned, so a leaked owned buffer aborts the test.
    static void runInNautilus(const std::function<void(nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*>)>& body)
    {
        auto engine = makeEngine(GetParam());
        auto function = engine.registerFunction(
            std::function([&body](nautilus::val<TupleBuffer*> onStackBuffer, nautilus::val<AbstractBufferProvider*> provider)
                          { body(onStackBuffer, provider); }));

        const auto bufferManager = BufferManager::create(TEST_BUFFER_SIZE, TEST_NUMBER_OF_BUFFERS);
        {
            TupleBuffer onStackBuffer = bufferManager->getBufferBlocking();
            onStackBuffer.setNumberOfTuples(ON_STACK_RECORD_COUNT);
            function(&onStackBuffer, bufferManager.get());
        }
        /// Everything the body allocated must have been released by now (the on-stack buffer went out of scope above).
        bufferManager->destroy();
    }
};

/// Asserts an equality across the nautilus boundary: the comparison runs on the concrete runtime values inside a `nautilus::invoke`,
/// and a mismatch throws, failing the surrounding test.
void expectRecordCount(const nautilus::val<size_t>& actual, const nautilus::val<size_t>& expected)
{
    nautilus::invoke(
        +[](size_t actualValue, size_t expectedValue)
        {
            ASSERT_EQ(actualValue, expectedValue)
                << fmt::format("NautilusBufferTest: expected {} records but got {}", expectedValue, actualValue);
        },
        actual,
        expected);
}

void expectValid(const nautilus::val<bool>& valid)
{
    nautilus::invoke(+[](bool isValid) { ASSERT_TRUE(isValid) << "NautilusBufferTest: expected the buffer to be valid"; }, valid);
}

/// Allocates a fresh owned buffer from `provider` following the intended pattern: empty-initialize on the stack, then assign inside a
/// `nautilus::invoke`. The returned buffer keeps its TupleBuffer alive until it goes out of scope.
OwnedNautilusBuffer allocateOwned(const nautilus::val<AbstractBufferProvider*>& provider, const nautilus::val<uint64_t>& numberOfRecords)
{
    OwnedNautilusBuffer owned;
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider, TupleBuffer* out, uint64_t records)
        {
            *out = bufferProvider->getBufferBlocking();
            out->setNumberOfTuples(records);
        },
        provider,
        owned.asArg(),
        numberOfRecords);
    return owned;
}

/// A consumer that is agnostic to buffer ownership: it works for OwnedNautilusBuffer, BorrowedNautilusBuffer and NautilusBuffer alike,
/// since all three expose the same read-only interface. This is the API shape the design intends most code to use.
template <typename Buffer>
nautilus::val<size_t> consume(const Buffer& buffer)
{
    /// Uses asArg() in its intended role - as an argument to a nautilus::invoke - which compiles for all three buffer types.
    return nautilus::invoke(+[](const TupleBuffer* tupleBuffer) { return tupleBuffer->getNumberOfTuples(); }, buffer.asArg());
}

/// A BorrowedNautilusBuffer reads through to the on-stack TupleBuffer it does not own.
TEST_P(NautilusBufferTest, BorrowedBufferReadsThroughToTupleBuffer)
{
    runInNautilus([](nautilus::val<TupleBuffer*> onStackBuffer, nautilus::val<AbstractBufferProvider*>)
                  { expectRecordCount(BorrowedNautilusBuffer::from(onStackBuffer).getNumberOfRecords(), ON_STACK_RECORD_COUNT); });
}

/// The intended construction of an OwnedNautilusBuffer: empty-init on the stack, then assign the buffer inside a nautilus::invoke.
TEST_P(NautilusBufferTest, OwnedBufferEmptyInitThenAssignInInvoke)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*> provider)
        {
            OwnedNautilusBuffer owned;
            expectValid(!owned.isValid());
            nautilus::invoke(
                +[](AbstractBufferProvider* bufferProvider, TupleBuffer* out) { *out = bufferProvider->getBufferBlocking(); },
                provider,
                owned.asArg());
            expectValid(owned.isValid());
        });
}

/// Move-constructing an OwnedNautilusBuffer transfers the wrapped TupleBuffer to the moved-into buffer.
TEST_P(NautilusBufferTest, OwnedBufferMoveConstruction)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*> provider)
        {
            auto owned = allocateOwned(provider, 7);
            const OwnedNautilusBuffer movedInto = std::move(owned);
            expectRecordCount(movedInto.getNumberOfRecords(), 7);
        });
}

/// Copy-constructing an OwnedNautilusBuffer shares the reference-counted TupleBuffer, so the copy observes the same records.
TEST_P(NautilusBufferTest, OwnedBufferCopyConstruction)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*> provider)
        {
            const auto owned = allocateOwned(provider, 7);
            const OwnedNautilusBuffer copied = owned;
            expectRecordCount(copied.getNumberOfRecords(), 7);
        });
}

/// Loading a child buffer from an OwnedNautilusBuffer yields a valid owned child buffer.
TEST_P(NautilusBufferTest, OwnedBufferStoreAndGetChild)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*> provider)
        {
            auto parent = allocateOwned(provider, 7);
            const auto childIndex = parent.storeChild(allocateOwned(provider, 3));
            expectValid(parent.getChild(childIndex).isValid());
        });
}

/// A NautilusBuffer can be constructed from a BorrowedNautilusBuffer and reports itself as not owned.
TEST_P(NautilusBufferTest, NautilusBufferFromBorrowed)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*> onStackBuffer, nautilus::val<AbstractBufferProvider*>)
        {
            const NautilusBuffer fromBorrowed{BorrowedNautilusBuffer::from(onStackBuffer)};
            expectRecordCount(fromBorrowed.getNumberOfRecords(), ON_STACK_RECORD_COUNT);
            EXPECT_FALSE(fromBorrowed.isOwned());
        });
}

/// A NautilusBuffer can be constructed from an OwnedNautilusBuffer and reports itself as owned.
TEST_P(NautilusBufferTest, NautilusBufferFromOwned)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*> provider)
        {
            const NautilusBuffer fromOwned{allocateOwned(provider, 5)};
            expectRecordCount(fromOwned.getNumberOfRecords(), 5);
            EXPECT_TRUE(fromOwned.isOwned());
        });
}

/// The ownership-agnostic consumer works on a BorrowedNautilusBuffer.
TEST_P(NautilusBufferTest, ConsumerOnBorrowed)
{
    runInNautilus([](nautilus::val<TupleBuffer*> onStackBuffer, nautilus::val<AbstractBufferProvider*>)
                  { expectRecordCount(consume(BorrowedNautilusBuffer::from(onStackBuffer)), ON_STACK_RECORD_COUNT); });
}

/// The ownership-agnostic consumer works on an OwnedNautilusBuffer.
TEST_P(NautilusBufferTest, ConsumerOnOwned)
{
    runInNautilus([](nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*> provider)
                  { expectRecordCount(consume(allocateOwned(provider, 5)), 5); });
}

/// The ownership-agnostic consumer works on a type-erased NautilusBuffer.
TEST_P(NautilusBufferTest, ConsumerOnNautilusBuffer)
{
    runInNautilus([](nautilus::val<TupleBuffer*> onStackBuffer, nautilus::val<AbstractBufferProvider*>)
                  { expectRecordCount(consume(NautilusBuffer{BorrowedNautilusBuffer::from(onStackBuffer)}), ON_STACK_RECORD_COUNT); });
}

/// OwnedNautilusBuffer::copy promotes a borrowed buffer into an owned one that references the same TupleBuffer.
TEST_P(NautilusBufferTest, CopyFromBorrowed)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*> onStackBuffer, nautilus::val<AbstractBufferProvider*>)
        {
            const auto owned = OwnedNautilusBuffer::copy(NautilusBuffer{BorrowedNautilusBuffer::from(onStackBuffer)});
            expectRecordCount(owned.getNumberOfRecords(), ON_STACK_RECORD_COUNT);
        });
}

/// OwnedNautilusBuffer::copy of an owned source yields an owned buffer referencing the same TupleBuffer.
TEST_P(NautilusBufferTest, CopyFromOwned)
{
    runInNautilus(
        [](nautilus::val<TupleBuffer*>, nautilus::val<AbstractBufferProvider*> provider)
        {
            const auto owned = OwnedNautilusBuffer::copy(NautilusBuffer{allocateOwned(provider, 5)});
            expectRecordCount(owned.getNumberOfRecords(), 5);
        });
}

INSTANTIATE_TEST_CASE_P(
    NautilusBufferTest,
    NautilusBufferTest,
    ::testing::Values(EngineMode::Interpreter, EngineMode::Compiler),
    [](const testing::TestParamInfo<EngineMode>& info)
    { return std::string{info.param == EngineMode::Compiler ? "Compiler" : "Interpreter"}; });

/// NOLINTEND(readability-magic-numbers, performance-unnecessary-value-param, performance-unnecessary-copy-initialization)

}
}
