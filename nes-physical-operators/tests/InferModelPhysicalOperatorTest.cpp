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

#include <InferModelPhysicalOperator.hpp>
#include <Inference/IREEInferenceOperatorHandler.hpp>
#include <ModelLoader.hpp>
#include <PhysicalOperator.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <ExecutionContext.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <PipelineExecutionContext.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <TestTupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A no-op physical operator used as a child so that InferModelPhysicalOperator's
/// setupChild/executeChild/terminateChild calls do not crash.
struct NoOpChildOperator final : PhysicalOperatorConcept
{
    void setup(ExecutionContext&, CompilationContext&) const override { /*noop*/ }
    void execute(ExecutionContext&, Record&) const override { /*noop*/ }
    void terminate(ExecutionContext&) const override { /*noop*/ }
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override { return std::nullopt; }
    void setChild(PhysicalOperator) override { /*noop*/ }
};

class InferModelPhysicalOperatorTest : public Testing::BaseUnitTest
{
protected:
    struct MockedPipelineContext final : PipelineExecutionContext
    {
        bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
        {
            buffers.wlock()->emplace_back(buffer);
            return true;
        }

        TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

        [[nodiscard]] WorkerThreadId getId() const override { return INITIAL<WorkerThreadId>; }

        [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

        [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

        [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override
        {
            return *operatorHandlers;
        }

        void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& opHandlers) override
        {
            operatorHandlers = &opHandlers;
        }

        void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override
        {
            INVARIANT(false, "This function should not be called");
        }

        ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the fixture
        folly::Synchronized<std::vector<TupleBuffer>>& buffers;
        std::shared_ptr<BufferManager> bufferManager;
        std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;

        MockedPipelineContext(
            folly::Synchronized<std::vector<TupleBuffer>>& buffers,
            std::shared_ptr<BufferManager> bufferManager)
            : buffers(buffers), bufferManager(std::move(bufferManager))
        {
        }
    };

public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("InferModelPhysicalOperatorTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup InferModelPhysicalOperatorTest test class.");

        if (!NES::Inference::enabled())
        {
            return;
        }

        const std::string base = std::string(INFERENCE_TEST_DATA) + "/";
        auto identityResult = NES::Inference::load(base + "tiny_identity.onnx", {});
        if (identityResult)
        {
            identityModel = std::move(*identityResult);
        }

        auto reductionResult = NES::Inference::load(base + "tiny_reduction.onnx", {});
        if (reductionResult)
        {
            reductionModel = std::move(*reductionResult);
        }

        auto expansionResult = NES::Inference::load(base + "tiny_expansion.onnx", {});
        if (expansionResult)
        {
            expansionModel = std::move(*expansionResult);
        }
    }

    void SetUp() override { BaseUnitTest::SetUp(); }

    /// Packs a vector of floats into a raw-byte std::string suitable for VARSIZED field input.
    static std::string packFloatsToVarsized(const std::vector<float>& floats)
    {
        const char* rawData = reinterpret_cast<const char*>(floats.data());
        return std::string(rawData, floats.size() * sizeof(float));
    }

    /// Generates a vector of N floats starting at startValue with step 1.0f
    static std::vector<float> makeFloats(size_t n, float startValue = 1.0f)
    {
        std::vector<float> v(n);
        std::iota(v.begin(), v.end(), startValue);
        return v;
    }

    /// Generates output field names "out_0", "out_1", ... "out_{n-1}"
    static std::vector<std::string> makeOutputFieldNames(size_t n)
    {
        std::vector<std::string> names;
        names.reserve(n);
        for (size_t i = 0; i < n; ++i)
        {
            names.push_back("out_" + std::to_string(i));
        }
        return names;
    }

    /// Reads float output value at field index from a Record.
    static float readOutputField(const Record& record, const std::string& fieldName)
    {
        const VarVal& val = record.read(fieldName);
        auto floatVal = val.cast<nautilus::val<float>>();
        return nautilus::details::RawValueResolver<float>::getRawValue(floatVal);
    }

    /// Runs the operator lifecycle (setup + loop over records + terminate) using varsized input.
    /// Returns the executed Records so callers can assert on output fields.
    ///
    /// The TupleBuffer is populated with the input schema (VARSIZED "input_blob" only).
    /// Records are read from the buffer projecting only "input_blob".
    /// InferModelPhysicalOperator::execute() writes output fields into the Record's in-memory map.
    /// After execute(), we read output fields from the Record directly.
    std::vector<Record> runVarsizedInference(
        const NES::Nebuli::Inference::Model& model,
        const std::vector<std::vector<float>>& inputsPerRecord,
        size_t numOutputFields)
    {
        const auto outputFieldNames = makeOutputFieldNames(numOutputFields);

        auto handler = std::make_shared<NES::Inference::IREEInferenceOperatorHandler>(model);
        handlers.insert_or_assign(OperatorHandlerId(0), handler);

        /// Schema with only the VARSIZED input field
        Schema inputSchema;
        inputSchema.addField("input_blob", DataType::Type::VARSIZED);

        InferModelPhysicalOperator op(OperatorHandlerId(0), {"input_blob"}, outputFieldNames, /*varsizedInput=*/true);
        op.setChild(PhysicalOperator(NoOpChildOperator{}));

        MockedPipelineContext pec{emittedBuffers, bm};
        pec.setOperatorHandlers(handlers);
        Arena arena(bm);
        ExecutionContext ctx{&pec, &arena};

        nautilus::engine::Options options;
        options.setOption("engine.Compilation", false);
        nautilus::engine::NautilusEngine engine(options);
        CompilationContext compilationCtx(engine);

        op.setup(ctx, compilationCtx);

        const size_t bufferSize = 65536;
        auto bufRef = LowerSchemaProvider::lowerSchema(bufferSize, inputSchema, MemoryLayoutType::ROW_LAYOUT);

        auto tupleBuffer = bm->getBufferBlocking();
        RecordBuffer recordBuffer(std::addressof(tupleBuffer));

        Testing::TestTupleBuffer ttb(inputSchema);
        auto view = ttb.open(tupleBuffer, bm.get());

        for (const auto& inputFloats : inputsPerRecord)
        {
            view.append(packFloatsToVarsized(inputFloats));
        }

        std::vector<Record> outputRecords;
        outputRecords.reserve(inputsPerRecord.size());

        const size_t numRecords = inputsPerRecord.size();
        for (size_t i = 0; i < numRecords; ++i)
        {
            nautilus::val<uint64_t> idx(i);
            Record record = bufRef->readRecord({"input_blob"}, recordBuffer, idx);
            op.execute(ctx, record);
            outputRecords.push_back(record);
        }

        op.terminate(ctx);

        return outputRecords;
    }

    folly::Synchronized<std::vector<TupleBuffer>> emittedBuffers;
    std::shared_ptr<BufferManager> bm = BufferManager::create(65536, 100);
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;

    static inline std::optional<NES::Nebuli::Inference::Model> identityModel;
    static inline std::optional<NES::Nebuli::Inference::Model> reductionModel;
    static inline std::optional<NES::Nebuli::Inference::Model> expansionModel;
};

/// PHYS-01 + PHYS-02 + PHYS-03:
/// Full lifecycle (setup->execute->terminate) completes without crash.
/// Identity model output equals input for all 100 positive float values.
TEST_F(InferModelPhysicalOperatorTest, FullLifecycleAndCorrectnessIdentity)
{
    if (!NES::Inference::enabled())
    {
        GTEST_SKIP() << "IREE tools not available";
    }
    ASSERT_TRUE(identityModel.has_value()) << "Identity model failed to load";

    constexpr size_t numFloats = 100;
    const auto inputFloats = makeFloats(numFloats);

    auto records = runVarsizedInference(*identityModel, {inputFloats}, numFloats);

    ASSERT_EQ(records.size(), 1u);
    const auto& record = records[0];

    for (size_t i = 0; i < numFloats; ++i)
    {
        const float expected = static_cast<float>(i + 1);
        const float actual = readOutputField(record, "out_" + std::to_string(i));
        EXPECT_NEAR(actual, expected, 1e-5f) << "Field out_" << i << " mismatch";
    }
}

/// PHYS-02: Reduction model produces 10 zeros for any 100-element input.
TEST_F(InferModelPhysicalOperatorTest, CorrectnessReduction)
{
    if (!NES::Inference::enabled())
    {
        GTEST_SKIP() << "IREE tools not available";
    }
    ASSERT_TRUE(reductionModel.has_value()) << "Reduction model failed to load";

    constexpr size_t numInputFloats = 100;
    constexpr size_t numOutputFloats = 10;
    const auto inputFloats = makeFloats(numInputFloats);

    auto records = runVarsizedInference(*reductionModel, {inputFloats}, numOutputFloats);

    ASSERT_EQ(records.size(), 1u);
    const auto& record = records[0];

    for (size_t i = 0; i < numOutputFloats; ++i)
    {
        const float actual = readOutputField(record, "out_" + std::to_string(i));
        EXPECT_NEAR(actual, 0.0f, 1e-5f) << "Field out_" << i << " expected zero from all-zero weight matrix";
    }
}

/// PHYS-02: Expansion model produces 100 zeros for any 10-element input.
TEST_F(InferModelPhysicalOperatorTest, CorrectnessExpansion)
{
    if (!NES::Inference::enabled())
    {
        GTEST_SKIP() << "IREE tools not available";
    }
    ASSERT_TRUE(expansionModel.has_value()) << "Expansion model failed to load";

    constexpr size_t numInputFloats = 10;
    constexpr size_t numOutputFloats = 100;
    const auto inputFloats = makeFloats(numInputFloats);

    auto records = runVarsizedInference(*expansionModel, {inputFloats}, numOutputFloats);

    ASSERT_EQ(records.size(), 1u);
    const auto& record = records[0];

    for (size_t i = 0; i < numOutputFloats; ++i)
    {
        const float actual = readOutputField(record, "out_" + std::to_string(i));
        EXPECT_NEAR(actual, 0.0f, 1e-5f) << "Field out_" << i << " expected zero from all-zero weight matrix";
    }
}

/// PHYS-04: Multi-record buffer with identity model — 5 records, per-record correctness.
TEST_F(InferModelPhysicalOperatorTest, MultiRecordIdentity)
{
    if (!NES::Inference::enabled())
    {
        GTEST_SKIP() << "IREE tools not available";
    }
    ASSERT_TRUE(identityModel.has_value()) << "Identity model failed to load";

    constexpr size_t numFloats = 100;
    constexpr size_t numRecords = 5;

    std::vector<std::vector<float>> inputs;
    inputs.reserve(numRecords);
    for (size_t r = 0; r < numRecords; ++r)
    {
        inputs.push_back(makeFloats(numFloats, static_cast<float>(r * numFloats + 1)));
    }

    auto records = runVarsizedInference(*identityModel, inputs, numFloats);

    ASSERT_EQ(records.size(), numRecords);
    for (size_t r = 0; r < numRecords; ++r)
    {
        for (size_t i = 0; i < numFloats; ++i)
        {
            const float expected = static_cast<float>(r * numFloats + i + 1);
            const float actual = readOutputField(records[r], "out_" + std::to_string(i));
            EXPECT_NEAR(actual, expected, 1e-5f) << "Record " << r << " field out_" << i << " mismatch";
        }
    }
}

/// PHYS-04: Multi-record buffer with reduction model — 5 records, all outputs are zeros.
TEST_F(InferModelPhysicalOperatorTest, MultiRecordReduction)
{
    if (!NES::Inference::enabled())
    {
        GTEST_SKIP() << "IREE tools not available";
    }
    ASSERT_TRUE(reductionModel.has_value()) << "Reduction model failed to load";

    constexpr size_t numInputFloats = 100;
    constexpr size_t numOutputFloats = 10;
    constexpr size_t numRecords = 5;

    std::vector<std::vector<float>> inputs;
    inputs.reserve(numRecords);
    for (size_t r = 0; r < numRecords; ++r)
    {
        inputs.push_back(makeFloats(numInputFloats, static_cast<float>(r * numInputFloats + 1)));
    }

    auto records = runVarsizedInference(*reductionModel, inputs, numOutputFloats);

    ASSERT_EQ(records.size(), numRecords);
    for (size_t r = 0; r < numRecords; ++r)
    {
        for (size_t i = 0; i < numOutputFloats; ++i)
        {
            const float actual = readOutputField(records[r], "out_" + std::to_string(i));
            EXPECT_NEAR(actual, 0.0f, 1e-5f) << "Record " << r << " field out_" << i << " expected zero";
        }
    }
}

/// PHYS-04: Multi-record buffer with expansion model — 5 records, all outputs are zeros.
TEST_F(InferModelPhysicalOperatorTest, MultiRecordExpansion)
{
    if (!NES::Inference::enabled())
    {
        GTEST_SKIP() << "IREE tools not available";
    }
    ASSERT_TRUE(expansionModel.has_value()) << "Expansion model failed to load";

    constexpr size_t numInputFloats = 10;
    constexpr size_t numOutputFloats = 100;
    constexpr size_t numRecords = 5;

    std::vector<std::vector<float>> inputs;
    inputs.reserve(numRecords);
    for (size_t r = 0; r < numRecords; ++r)
    {
        inputs.push_back(makeFloats(numInputFloats, static_cast<float>(r * numInputFloats + 1)));
    }

    auto records = runVarsizedInference(*expansionModel, inputs, numOutputFloats);

    ASSERT_EQ(records.size(), numRecords);
    for (size_t r = 0; r < numRecords; ++r)
    {
        for (size_t i = 0; i < numOutputFloats; ++i)
        {
            const float actual = readOutputField(records[r], "out_" + std::to_string(i));
            EXPECT_NEAR(actual, 0.0f, 1e-5f) << "Record " << r << " field out_" << i << " expected zero";
        }
    }
}

/// PHYS-07: Zero-tuple buffer — operator setup and terminate with no records to process.
/// No crash, no emitted buffers.
/// PHYS-05 and PHYS-06 are DROPPED per user decision (framework guarantees ordering and single terminate).
TEST_F(InferModelPhysicalOperatorTest, ZeroTupleBuffer)
{
    if (!NES::Inference::enabled())
    {
        GTEST_SKIP() << "IREE tools not available";
    }
    ASSERT_TRUE(identityModel.has_value()) << "Identity model failed to load";

    constexpr size_t numOutputFloats = 100;
    const auto outputFieldNames = makeOutputFieldNames(numOutputFloats);

    auto handler = std::make_shared<NES::Inference::IREEInferenceOperatorHandler>(*identityModel);
    handlers.insert_or_assign(OperatorHandlerId(0), handler);

    InferModelPhysicalOperator op(OperatorHandlerId(0), {"input_blob"}, outputFieldNames, /*varsizedInput=*/true);
    op.setChild(PhysicalOperator(NoOpChildOperator{}));

    MockedPipelineContext pec{emittedBuffers, bm};
    pec.setOperatorHandlers(handlers);
    Arena arena(bm);
    ExecutionContext ctx{&pec, &arena};

    nautilus::engine::Options options;
    options.setOption("engine.Compilation", false);
    nautilus::engine::NautilusEngine engine(options);
    CompilationContext compilationCtx(engine);

    /// Setup with zero records — no execute() calls, just setup + terminate
    op.setup(ctx, compilationCtx);
    /// (no execute loop — numberOfTuples == 0)
    op.terminate(ctx);

    EXPECT_EQ(emittedBuffers.rlock()->size(), 0u) << "Zero-tuple buffer must not emit any output";
}

} /// namespace NES
