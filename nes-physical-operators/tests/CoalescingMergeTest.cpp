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

#include <cstddef>
#include <cstdint>
#include <exception> /// NOLINT(misc-include-cleaner): rapidcheck uses std::exception_ptr without including it
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include <rapidcheck.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <nautilus/Engine.hpp>
#include <rapidcheck/gtest.h>
#include <BaseUnitTest.hpp>
#include <CoalescingEmitOperatorHandler.hpp>
#include <CompilationContext.hpp>
#include <DataStructureTestUtils.hpp>
#include <TestTupleBuffer.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
using TestUtils::EngineMode;

constexpr uint64_t BUFFER_SIZE = 1024;
constexpr uint64_t NUMBER_OF_BUFFERS = 1024;
constexpr size_t MAX_FIELDS = 6;

/// The logger can be set up only once per process, and property tests run outside the fixtures.
/// Without console logging, a violated precondition prints its message to stderr, where death tests match it.
void ensureLoggingSetup()
{
    static std::once_flag flag;
    std::call_once(flag, [] { Logger::setupLogging("CoalescingMergeTest.log", LogLevel::LOG_DEBUG, false); });
}

std::string engineModeName(const EngineMode mode)
{
    return mode == EngineMode::Compiler ? "Compiler" : "Interpreter";
}

/// Registers `mergeRecordsFunction` in a module and compiles it, as a pipeline does.
auto compileMerge(const std::shared_ptr<TupleBufferRef>& bufferRef, const EngineMode mode)
{
    const auto engine = TestUtils::makeEngine(mode);
    auto module = engine.createModule();
    CompilationContext compilationContext{module};
    auto merge = compilationContext.registerFunction(mergeRecordsFunction(bufferRef), "coalesceMerge");
    auto compiledModule = module.compile();
    compilationContext.resolveAfterCompilation(compiledModule);
    return merge;
}

/// Writes `record` behind the tuples of `buffer` through `ref`.
void appendRecord(
    const TupleBufferRef& ref,
    TupleBuffer& buffer,
    AbstractBufferProvider& bufferProvider,
    TestUtils::AnyVec& record,
    const std::vector<Record::RecordFieldIdentifier>& names,
    const std::vector<DataType>& types)
{
    const RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(nautilus::val<TupleBuffer*>(std::addressof(buffer)))};
    nautilus::val<uint64_t> index(buffer.getNumberOfTuples());
    ref.writeRecord(
        index,
        recordBuffer,
        TestUtils::buildRecordFromAnyVec(nautilus::val<TestUtils::AnyVec*>(std::addressof(record)), names, types),
        nautilus::val<AbstractBufferProvider*>(std::addressof(bufferProvider)));
    buffer.setNumberOfTuples(buffer.getNumberOfTuples() + 1);
}

TestUtils::AnyVec readRecord(
    const TupleBufferRef& ref,
    TupleBuffer& buffer,
    const uint64_t index,
    const std::vector<Record::RecordFieldIdentifier>& names,
    const std::vector<DataType>& types)
{
    const RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(nautilus::val<TupleBuffer*>(std::addressof(buffer)))};
    nautilus::val<uint64_t> recordIndex(index);
    TestUtils::AnyVec record(types.size());
    TestUtils::storeRecordToAnyVec(
        nautilus::val<TestUtils::AnyVec*>(std::addressof(record)), ref.readRecord(names, recordBuffer, recordIndex), names, types);
    return record;
}

Testing::TestSchema keyAndPayload()
{
    return Testing::TestSchema{
        UnqualifiedUnboundField{Identifier::parse("key"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("payload"), DataType::Type::VARSIZED}};
}

TupleBuffer withPayloads(BufferManager& bufferManager, const std::vector<std::string>& payloads, const MemoryLayoutType layoutType)
{
    auto buffer = bufferManager.getBufferBlocking();
    auto view = Testing::TestTupleBuffer{keyAndPayload(), layoutType}.open(buffer, &bufferManager);
    for (uint64_t key = 0; key < payloads.size(); ++key)
    {
        view.append(key, payloads[key]);
    }
    return buffer;
}

std::vector<std::string> payloadsIn(TupleBuffer& buffer, BufferManager& bufferManager, const MemoryLayoutType layoutType)
{
    auto view = Testing::TestTupleBuffer{keyAndPayload(), layoutType}.open(buffer, &bufferManager);
    std::vector<std::string> payloads;
    payloads.reserve(view.getNumberOfTuples());
    for (size_t i = 0; i < view.getNumberOfTuples(); ++i)
    {
        payloads.push_back(view[i]["payload"].as<std::string>());
    }
    return payloads;
}

/// Records written through a row or columnar layout, split across buffers and merged back, read the same as before.
void mergedBuffersKeepEveryRecordProperty(const EngineMode mode)
{
    const auto types = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, MAX_FIELDS);
    const auto layoutType = *rc::gen::element(MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT);
    const auto ref = LowerSchemaProvider::lowerSchema(BUFFER_SIZE, TestUtils::createSchemaFromDataTypes(types), layoutType);
    const auto names = ref->getAllFieldNames();
    const auto merge = compileMerge(ref, mode);
    const auto sourceSizes = *rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, ref->getCapacity() + 1));

    const auto bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
    std::vector<TestUtils::AnyVec> reference;
    std::vector<TupleBuffer> merged{bufferManager->getBufferBlocking()};
    for (const auto size : sourceSizes)
    {
        auto source = bufferManager->getBufferBlocking();
        for (uint64_t i = 0; i < size; ++i)
        {
            reference.push_back(*TestUtils::genAnyVec(types));
            appendRecord(*ref, source, *bufferManager, reference.back(), names, types);
        }
        if (merged.back().getNumberOfTuples() + size > ref->getCapacity())
        {
            merged.push_back(bufferManager->getBufferBlocking());
        }
        merge(&merged.back(), &source, bufferManager.get());
    }

    size_t next = 0;
    for (auto& buffer : merged)
    {
        for (uint64_t i = 0; i < buffer.getNumberOfTuples(); ++i)
        {
            RC_ASSERT(next < reference.size());
            RC_ASSERT(TestUtils::anyVecsEqual(readRecord(*ref, buffer, i, names, types), reference[next], types));
            ++next;
        }
    }
    RC_ASSERT(next == reference.size());
    merged.clear();
    /// A leaked buffer keeps the buffer manager alive, so only an explicit `destroy` detects it.
    bufferManager->destroy();
}
}

RC_GTEST_PROP(CoalescingMergePropertyTest, MergedBuffersKeepEveryRecordCompiler, ())
{
    ensureLoggingSetup();
    mergedBuffersKeepEveryRecordProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(CoalescingMergePropertyTest, MergedBuffersKeepEveryRecordInterpreter, ())
{
    ensureLoggingSetup();
    mergedBuffersKeepEveryRecordProperty(EngineMode::Interpreter);
}

class CoalescingMergeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { ensureLoggingSetup(); }
};

class CoalescingMergeLayoutTest : public CoalescingMergeTest, public testing::WithParamInterface<std::tuple<EngineMode, MemoryLayoutType>>
{
};

class CoalescingMergeDeathTest : public CoalescingMergeTest, public testing::WithParamInterface<EngineMode>
{
};

/// The last payload does not fit behind the others in the target's child buffer.
TEST_P(CoalescingMergeLayoutTest, PayloadsAreCopiedIntoTheChildBuffersOfTheTarget)
{
    const auto [mode, layoutType] = GetParam();
    const auto merge = compileMerge(LowerSchemaProvider::lowerSchema(BUFFER_SIZE, keyAndPayload(), layoutType), mode);
    const auto bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
    const std::string large(BUFFER_SIZE - 8, 'x');
    auto target = withPayloads(*bufferManager, {"abcd", "ef"}, layoutType);
    {
        const auto source = withPayloads(*bufferManager, {"wxyz", large}, layoutType);
        merge(&target, &source, bufferManager.get());
    }

    EXPECT_EQ(target.getNumberOfChildBuffers(), 2);
    EXPECT_EQ(payloadsIn(target, *bufferManager, layoutType), (std::vector<std::string>{"abcd", "ef", "wxyz", large}));
    target.release();
    bufferManager->destroy();
}

/// The payload does not fit into a pooled buffer.
TEST_P(CoalescingMergeLayoutTest, OversizedPayloadsAreCopiedIntoUnpooledChildBuffers)
{
    const auto [mode, layoutType] = GetParam();
    const auto merge = compileMerge(LowerSchemaProvider::lowerSchema(BUFFER_SIZE, keyAndPayload(), layoutType), mode);
    const auto bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
    const std::string oversized(2 * BUFFER_SIZE, 'x');
    auto target = withPayloads(*bufferManager, {"abcd"}, layoutType);
    {
        const auto source = withPayloads(*bufferManager, {oversized}, layoutType);
        merge(&target, &source, bufferManager.get());
    }

    EXPECT_EQ(payloadsIn(target, *bufferManager, layoutType), (std::vector<std::string>{"abcd", oversized}));
    target.release();
    bufferManager->destroy();
}

TEST_P(CoalescingMergeDeathTest, RejectsRecordsThatDoNotFit)
{
    SKIP_IF_TSAN();
    /// A forked child would lack the fixture's logger thread.
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto ref = LowerSchemaProvider::lowerSchema(BUFFER_SIZE, keyAndPayload(), MemoryLayoutType::ROW_LAYOUT);
    const auto merge = compileMerge(ref, GetParam());
    const auto bufferManager = TestUtils::createBufferManager(BUFFER_SIZE, NUMBER_OF_BUFFERS);
    auto target = bufferManager->getBufferBlocking();
    target.setNumberOfTuples(ref->getCapacity());
    const auto source = withPayloads(*bufferManager, {"abcd"}, MemoryLayoutType::ROW_LAYOUT);
    EXPECT_DEATH_DEBUG(merge(&target, &source, bufferManager.get()), "A merge needs room for");
}

INSTANTIATE_TEST_SUITE_P(
    InterpreterAndCompiler,
    CoalescingMergeLayoutTest,
    testing::Combine(
        testing::Values(EngineMode::Interpreter, EngineMode::Compiler),
        testing::Values(MemoryLayoutType::ROW_LAYOUT, MemoryLayoutType::COLUMNAR_LAYOUT)),
    [](const testing::TestParamInfo<std::tuple<EngineMode, MemoryLayoutType>>& info)
    { return engineModeName(std::get<0>(info.param)) + (std::get<1>(info.param) == MemoryLayoutType::ROW_LAYOUT ? "Row" : "Columnar"); });

INSTANTIATE_TEST_SUITE_P(
    InterpreterAndCompiler,
    CoalescingMergeDeathTest,
    testing::Values(EngineMode::Interpreter, EngineMode::Compiler),
    [](const testing::TestParamInfo<EngineMode>& info) { return engineModeName(info.param); });

}
