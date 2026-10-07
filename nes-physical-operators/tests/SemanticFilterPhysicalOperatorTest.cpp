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

#include <SemanticFilterPhysicalOperator.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <PipelineExecutionContext.hpp>
#include <ScanPhysicalOperator.hpp>
#include <SemanticBackend.hpp>
#include <SemanticBackendFactory.hpp>
#include <SemanticModelCatalog.hpp>
#include <TestTupleBuffer.hpp>
#include <options.hpp>

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers, bugprone-unchecked-optional-access)

namespace
{

constexpr size_t BufferSize = 8192;
constexpr uint32_t NumberOfPooledBuffers = 200;
constexpr BufferAlignment Alignment{64};
constexpr double UnpooledMemoryFraction = 0.9;
constexpr size_t TotalMemoryInBytes = 10 * static_cast<size_t>(NumberOfPooledBuffers) * BufferSize;

using TestSchema = Schema<UnqualifiedUnboundField, Ordered>;
using Row = std::unordered_map<std::string, std::string>;

std::shared_ptr<BufferManager> createBufferManager(const size_t totalMemory = TotalMemoryInBytes)
{
    return BufferManager::create(totalMemory, UnpooledMemoryFraction, Alignment, BufferSize, std::make_shared<NesDefaultMemoryAllocator>());
}

UnqualifiedUnboundField varsized(const std::string_view name)
{
    return UnqualifiedUnboundField{
        Identifier::parse(std::string{name}), DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}};
}

std::vector<QualifiedIdentifier> toIds(const std::vector<std::string>& names)
{
    return names | std::views::transform([](const std::string& name) { return QualifiedIdentifier::tryParse(name).value(); })
        | std::ranges::to<std::vector>();
}

SemanticStep filterStep()
{
    return SemanticStep{
        .kind = SemanticStep::Kind::FILTER, .prompt = "The review is positive", .outputColumn = {}, .outputValues = {}, .defaultValue = {}};
}

/// A filter model; `endpoint` picks the mock behaviour. Extra steps (a fused map) go in front.
SemanticModelConfig modelConfig(std::string endpoint, std::vector<SemanticStep> steps = {filterStep()})
{
    SemanticModelConfig config;
    config.backend = "mock";
    config.endpoint = std::move(endpoint);
    config.modelName = "mock-model";
    config.steps = std::move(steps);
    return config;
}

SemanticBackendProvider realFactory(const SemanticModelConfig& config)
{
    return [config] { return SemanticBackendFactory::create(config, std::nullopt); };
}

/// Answers every request with a malformed chat completion (a 2xx that is no envelope).
struct MalformedBackend final : SemanticBackend
{
    std::expected<std::string, BackendError> complete(const CompletionRequest&) override
    {
        return std::unexpected{BackendError{.kind = BackendError::Kind::MALFORMED_RESPONSE, .message = "no envelope"}};
    }
};

struct MockedPipelineContext final : PipelineExecutionContext
{
    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        buffers.wlock()->emplace_back(buffer);
        return true;
    }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return threadId; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return numberOfWorkerThreads; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return *operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        operatorHandlers = &handlers;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

    ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime is ensured by the test
    folly::Synchronized<std::vector<TupleBuffer>>& buffers;
    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* operatorHandlers = nullptr;
    WorkerThreadId threadId;
    uint64_t numberOfWorkerThreads;

    MockedPipelineContext(
        folly::Synchronized<std::vector<TupleBuffer>>& buffers,
        std::shared_ptr<BufferManager> bufferManager,
        const WorkerThreadId threadId = INITIAL<WorkerThreadId>,
        const uint64_t numberOfWorkerThreads = 1)
        : buffers(buffers), bufferManager(std::move(bufferManager)), threadId(threadId), numberOfWorkerThreads(numberOfWorkerThreads)
    {
    }
};

nautilus::engine::Options engineOptions(const bool compiled)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", compiled);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    return options;
}

/// Scan -> SemanticFilter -> Emit over one VARSIZED DESCRIPTION field, appending `outputNames`
/// (the fused map steps' columns, if any).
class SemanticFilterPipeline
{
public:
    SemanticFilterPipeline(SemanticBackendProvider provider, const SemanticModelConfig& config, std::vector<std::string> outputNames = {})
        : inputSchema(TestSchema{varsized("DESCRIPTION")})
        , outputSchema(
              [&outputNames]
              {
                  std::vector fields{varsized("DESCRIPTION")};
                  for (const auto& name : outputNames)
                  {
                      fields.push_back(varsized(name));
                  }
                  return fields | std::ranges::to<TestSchema>();
              }())
        , outputNames(outputNames)
    {
        const auto toSchemaIds = [](const TestSchema& schema)
        {
            return schema
                | std::views::transform([](const UnqualifiedUnboundField& field)
                                        { return static_cast<QualifiedIdentifier>(field.getFullyQualifiedName()); })
                | std::ranges::to<std::vector>();
        };
        ScanPhysicalOperator scan(
            LowerSchemaProvider::lowerSchema(BufferSize, inputSchema, MemoryLayoutType::ROW_LAYOUT), toSchemaIds(inputSchema));
        SemanticFilterPhysicalOperator filter(std::move(provider), config, toIds({"DESCRIPTION"}), toIds(outputNames));
        const EmitPhysicalOperator emit(
            OperatorHandlerId(1), LowerSchemaProvider::lowerSchema(BufferSize, outputSchema, MemoryLayoutType::ROW_LAYOUT));
        filter.setChild(PhysicalOperator(emit));
        scan.setChild(PhysicalOperator(filter));
        pipeline = std::make_shared<Pipeline>(PhysicalOperator(scan));
        handlers[OperatorHandlerId(1)] = std::make_shared<EmitOperatorHandler>();
    }

    /// Runs `texts` through a fresh stage on one worker thread and returns the emitted records.
    [[nodiscard]] std::vector<Row> run(const std::vector<std::string>& texts, const bool compiled)
    {
        auto input = inputBufferManager->getBufferBlocking();
        input.setSequenceNumber(SequenceNumber(1));
        input.setChunkNumber(INITIAL_CHUNK_NUMBER);
        input.setLastChunk(true);
        input.setOriginId(INITIAL<OriginId>);
        Testing::TestTupleBuffer inputView(inputSchema);
        auto writer = inputView.open(input, inputBufferManager.get());
        for (const auto& text : texts)
        {
            writer.append(text);
        }

        CompiledExecutablePipelineStage stage(pipeline, handlers, engineOptions(compiled));
        folly::Synchronized<std::vector<TupleBuffer>> emitted;
        MockedPipelineContext pec{emitted, createBufferManager()};
        stage.start(pec);
        stage.execute(input, pec);
        stage.stop(pec);

        std::vector<Row> result;
        for (auto buffer : *emitted.rlock())
        {
            Testing::TestTupleBuffer testBuffer(outputSchema);
            auto view = testBuffer.open(buffer);
            for (size_t index = 0; index < view.getNumberOfTuples(); ++index)
            {
                Row row;
                row["DESCRIPTION"] = view[index]["DESCRIPTION"].as<std::string>();
                for (const auto& name : outputNames)
                {
                    row[name] = view[index][name].as<std::string>();
                }
                result.push_back(std::move(row));
            }
        }
        return result;
    }

    TestSchema inputSchema;
    TestSchema outputSchema;
    std::vector<std::string> outputNames;
    std::shared_ptr<BufferManager> inputBufferManager = createBufferManager();
    std::shared_ptr<Pipeline> pipeline;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
};

std::vector<std::string> descriptionsOf(const std::vector<Row>& rows)
{
    auto descriptions = rows | std::views::transform([](const Row& row) { return row.at("DESCRIPTION"); }) | std::ranges::to<std::vector>();
    std::ranges::sort(descriptions);
    return descriptions;
}

}

class SemanticFilterPhysicalOperatorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticFilterPhysicalOperatorTest.log", LogLevel::LOG_DEBUG); }
};

/// The echo mock answers with the text upper-cased, so the record text is its own verdict.
TEST_F(SemanticFilterPhysicalOperatorTest, KeepsTheAffirmedRecordsOnly)
{
    const auto config = modelConfig("echo");
    for (const bool compiled : {false, true})
    {
        SemanticFilterPipeline pipeline{realFactory(config), config};
        const auto rows = pipeline.run({"true", "no", "yes", "false", "maybe", "TRUE"}, compiled);
        EXPECT_EQ(descriptionsOf(rows), (std::vector<std::string>{"TRUE", "true", "yes"})) << "compiled=" << compiled;
    }
}

TEST_F(SemanticFilterPhysicalOperatorTest, ConstantVerdicts)
{
    for (const bool compiled : {false, true})
    {
        const auto keep = modelConfig("label:true");
        SemanticFilterPipeline keepAll{realFactory(keep), keep};
        EXPECT_EQ(keepAll.run({"a", "b"}, compiled).size(), 2) << "compiled=" << compiled;

        const auto drop = modelConfig("label:false");
        SemanticFilterPipeline dropAll{realFactory(drop), drop};
        EXPECT_TRUE(dropAll.run({"a", "b"}, compiled).empty()) << "compiled=" << compiled;
    }
}

/// Unusable answers drop the record — the reference's falsy default — but do not fail the query.
TEST_F(SemanticFilterPhysicalOperatorTest, UnusableResponsesDropTheRecord)
{
    const auto config = modelConfig("unparseable");
    for (const bool compiled : {false, true})
    {
        SemanticFilterPipeline unparseable{realFactory(config), config};
        EXPECT_TRUE(unparseable.run({"true", "yes"}, compiled).empty());

        SemanticFilterPipeline malformed{[] { return std::make_unique<MalformedBackend>(); }, config};
        EXPECT_TRUE(malformed.run({"true"}, compiled).empty());
    }
}

TEST_F(SemanticFilterPhysicalOperatorTest, TransportFailureThrows)
{
    const auto config = modelConfig("fail");
    for (const bool compiled : {false, true})
    {
        SemanticFilterPipeline pipeline{realFactory(config), config};
        ASSERT_EXCEPTION_ERRORCODE((void)pipeline.run({"true"}, compiled), ErrorCode::InferenceRuntimeFailure);
    }
}

TEST_F(SemanticFilterPhysicalOperatorTest, ZeroRecordBufferEmitsNothing)
{
    const auto config = modelConfig("fail");
    for (const bool compiled : {false, true})
    {
        /// 'fail' would throw if a request were made at all.
        SemanticFilterPipeline pipeline{realFactory(config), config};
        EXPECT_TRUE(pipeline.run({}, compiled).empty());
    }
}

/// A map fused into the filter writes its column on the records that pass.
TEST_F(SemanticFilterPhysicalOperatorTest, FusedMapStepsWriteTheirColumn)
{
    const auto mapStep = SemanticStep{
        .kind = SemanticStep::Kind::MAP, .prompt = "Classify", .outputColumn = "SENTIMENT", .outputValues = {}, .defaultValue = {}};
    const auto config = modelConfig("echo", {mapStep, filterStep()});
    for (const bool compiled : {false, true})
    {
        SemanticFilterPipeline pipeline{realFactory(config), config, {"SENTIMENT"}};
        const auto rows = pipeline.run({"yes", "no", "true"}, compiled);
        ASSERT_EQ(rows.size(), 2) << "compiled=" << compiled;
        std::unordered_map<std::string, std::string> answers;
        for (const auto& row : rows)
        {
            answers[row.at("DESCRIPTION")] = row.at("SENTIMENT");
        }
        EXPECT_EQ(answers.at("yes"), "YES");
        EXPECT_EQ(answers.at("true"), "TRUE");
    }
}

/// NOLINTEND(readability-magic-numbers, bugprone-unchecked-optional-access)

}
