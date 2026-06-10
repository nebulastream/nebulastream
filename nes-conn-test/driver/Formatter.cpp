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

#include <Formatter.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <nautilus/options.hpp>
#include <EmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <FormatterPEC.hpp>
#include <InputFormatterDescriptor.hpp>
#include <InputFormatterProvider.hpp>
#include <InputFormatterValidationProvider.hpp>
#include <Pipeline.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES::ConnTest
{

namespace
{
/// JSONL object keys are the schema field *identifiers*, and they must line up with
/// the keys the conn-test runner emits/parses. Two engine-side transforms get in
/// the way: a logical-source schema *qualifies* names (`source$field`), and the SQL
/// binder *case-folds* unquoted identifiers (`num_records` -> `NUM_RECORDS`). The
/// runner instead speaks bare, lowercased names. Canonicalize both sides to the same
/// form here — strip any qualifier, then lowercase — so a native -> JSON buffer
/// carries `field` and a JSON -> native buffer matches the runner's keys, whatever
/// the SQL casing/quoting was. The native row layout is name-independent (offsets
/// follow the field types/order), so this changes only the JSON dialect, never the
/// tuple memory layout.
Schema canonicalizeFieldNames(const Schema& schema)
{
    Schema canonical;
    for (const auto& field : schema)
    {
        const auto separator = field.name.find_last_of('$');
        auto name = separator == std::string::npos ? field.name : field.name.substr(separator + 1);
        std::ranges::transform(
            name, name.begin(), [](const unsigned char character) { return static_cast<char>(std::tolower(character)); });
        canonical.addField(name, field.dataType);
    }
    return canonical;
}

/// Number of VARSIZED fields in `schema` — each contributes one child buffer per
/// tuple to a decode's pool high-water mark.
std::size_t varSizedFieldCount(const Schema& schema)
{
    std::size_t count = 0;
    for (const auto& field : schema)
    {
        if (field.dataType.isType(DataType::Type::VARSIZED))
        {
            ++count;
        }
    }
    return count;
}

/// Buffers a decode pool must hold at once: one per emitted native buffer, plus
/// (for a VARSIZED schema) one child buffer per variable-sized field per tuple — all
/// live simultaneously while the decoded buffers are held — plus slack.
std::size_t decodePoolBuffers(const std::size_t totalRows, const std::size_t rowsPerBuffer, const std::size_t varSizedFields)
{
    constexpr std::size_t slackBuffers = 64;
    const auto nativeBuffers = rowsPerBuffer > 0 ? ((totalRows + rowsPerBuffer - 1) / rowsPerBuffer) : 1;
    return nativeBuffers + (totalRows * varSizedFields) + slackBuffers;
}

/// Reconstruct the JSONL text the encode emitted: a formatted buffer's bytes are its
/// payload (numberOfTuples == byte count), spilling into child buffers. Mirrors how
/// FileSink / BufferIterator drain a formatted buffer.
std::string extractText(const std::vector<TupleBuffer>& buffers)
{
    std::string text;
    for (const auto& buffer : buffers)
    {
        text.append(buffer.getAvailableMemoryArea<char>().data(), buffer.getNumberOfTuples());
        for (std::uint32_t childIdx = 0; childIdx < buffer.getNumberOfChildBuffers(); ++childIdx)
        {
            auto child = buffer.loadChildBuffer(VariableSizedAccess::Index(childIdx));
            text.append(child.getAvailableMemoryArea<char>().data(), child.getNumberOfTuples());
        }
    }
    return text;
}

/// Interpreted (not MLIR-compiled) execution: this converter favours fast, single
/// threaded setup over throughput, and correctness is identical either way.
nautilus::engine::Options interpreterOptions()
{
    auto options = nautilus::engine::Options{};
    options.setOption("engine.Compilation", false);
    options.setOption("mlir.enableMultithreading", false);
    return options;
}

/// JSONL bytes -> native row layout: a JSON input-formatter scan feeding a
/// row-layout emit. Mirrors InputFormatterTestUtil::createInputFormatter.
std::shared_ptr<CompiledExecutablePipelineStage> makeDecodeStage(const Schema& schema, const uint64_t bufferSize)
{
    constexpr OperatorHandlerId emitHandlerId = INITIAL<OperatorHandlerId>;
    auto rowProvider = LowerSchemaProvider::lowerSchema(bufferSize, schema, MemoryLayoutType::ROW_LAYOUT);
    const auto parserConfig = InputFormatterValidationProvider::provide("JSON", {{"tuple_delimiter", "\n"}});
    INVARIANT(parserConfig.has_value(), "JSON is a built-in input formatter; its config must validate");

    auto scanOp
        = ScanPhysicalOperator(provideInputFormatter(InputFormatterDescriptor{"JSON", *parserConfig}, rowProvider), schema.getFieldNames());
    scanOp.setChild(EmitPhysicalOperator(emitHandlerId, std::move(rowProvider)));

    auto pipeline = std::make_shared<Pipeline>(std::move(scanOp));
    pipeline->getOperatorHandlers().emplace(emitHandlerId, std::make_shared<EmitOperatorHandler>());
    return std::make_shared<CompiledExecutablePipelineStage>(pipeline, pipeline->getOperatorHandlers(), interpreterOptions());
}

/// Native row layout -> JSONL bytes: a row-layout scan feeding a JSON
/// output-formatter emit. The JSON output formatter needs no config keys.
std::shared_ptr<CompiledExecutablePipelineStage> makeEncodeStage(const Schema& schema, const uint64_t bufferSize)
{
    constexpr OperatorHandlerId emitHandlerId = INITIAL<OperatorHandlerId>;
    auto rowProvider = LowerSchemaProvider::lowerSchema(bufferSize, schema, MemoryLayoutType::ROW_LAYOUT);
    auto jsonProvider = LowerSchemaProvider::lowerSchemaWithOutputFormat(bufferSize, schema, "JSON", {});

    auto scanOp = ScanPhysicalOperator(std::move(rowProvider), schema.getFieldNames());
    scanOp.setChild(EmitPhysicalOperator(emitHandlerId, std::move(jsonProvider)));

    auto pipeline = std::make_shared<Pipeline>(std::move(scanOp));
    pipeline->getOperatorHandlers().emplace(emitHandlerId, std::make_shared<EmitOperatorHandler>());
    return std::make_shared<CompiledExecutablePipelineStage>(pipeline, pipeline->getOperatorHandlers(), interpreterOptions());
}
}

struct Formatter::Direction
{
    std::shared_ptr<CompiledExecutablePipelineStage> stage;
    /// Collects what the emit writes (worker 0); a harness-owned production PEC,
    /// not the executable-tests double.
    std::shared_ptr<FormatterPEC> pec;

    Direction(std::shared_ptr<CompiledExecutablePipelineStage> stage, const std::shared_ptr<BufferManager>& bufferManager)
        : stage(std::move(stage)), pec(std::make_shared<FormatterPEC>(bufferManager))
    {
        this->stage->start(*this->pec);
    }
};

std::vector<TupleBuffer> Formatter::runStage(Direction& direction, const TupleBuffer* buffer)
{
    /// Release the previous call's emitted buffers before this call appends its own,
    /// so a streaming caller (the source encode) does not pin every buffer it ever
    /// produced. The stage's internal streaming state (partial records, sequence
    /// tracking) lives in the stage, not here, so it survives this reset.
    direction.pec->collected().clear();
    if (buffer != nullptr)
    {
        direction.stage->execute(*buffer, *direction.pec);
    }
    else
    {
        direction.stage->stop(*direction.pec);
    }
    return direction.pec->collected();
}

Formatter::Formatter(const Schema& schema, std::shared_ptr<BufferManager> bufferManager)
    : schema(canonicalizeFieldNames(schema))
    , bufferManager(std::move(bufferManager))
    , decodeDir(std::make_unique<Direction>(makeDecodeStage(this->schema, this->bufferManager->getBufferSize()), this->bufferManager))
    , encodeDir(std::make_unique<Direction>(makeEncodeStage(this->schema, this->bufferManager->getBufferSize()), this->bufferManager))
{
}

Formatter::~Formatter() = default;

std::vector<TupleBuffer> Formatter::decode(const std::string_view jsonl)
{
    /// Feed the whole blob as one (unpooled, so any size) input buffer with a valid
    /// single-chunk identity, then flush the input formatter's end-of-stream tail.
    auto inputOpt = bufferManager->getUnpooledBuffer(std::max<std::size_t>(jsonl.size(), 1));
    if (!inputOpt)
    {
        throw BufferAllocationFailure("Failed to get unpooled buffer for {} bytes of JSONL input", jsonl.size());
    }
    auto input = std::move(inputOpt.value());
    std::memcpy(input.getAvailableMemoryArea<char>().data(), jsonl.data(), jsonl.size());
    input.setNumberOfTuples(jsonl.size());
    input.setSequenceNumber(INITIAL<SequenceNumber>);
    input.setChunkNumber(INITIAL_CHUNK_NUMBER);

    auto decoded = runStage(*decodeDir, &input);
    auto tail = runStage(*decodeDir, nullptr);
    decoded.insert(decoded.end(), tail.begin(), tail.end());
    return decoded;
}

std::string Formatter::encodeToText(const TupleBuffer& native)
{
    /// The buffer arrives fully stamped by SourceThread::addBufferMetaData — tuple count,
    /// a monotonic sequence number, INITIAL_CHUNK_NUMBER, and lastChunk — which the encode
    /// scan reads directly (and the emit requires a valid, non-INVALID chunk number). The
    /// harness sets no metadata here; it just runs the stage and reassembles the text.
    return extractText(runStage(*encodeDir, &native));
}

Formatter::Decoded Formatter::decodeJsonl(const Schema& schema, const std::size_t bufferSize, const std::string_view jsonl)
{
    const auto rowWidth = std::max<std::size_t>(schema.getSizeOfSchemaInBytes(), 1);
    const auto rowsPerBuffer = std::max<std::size_t>(bufferSize / rowWidth, 1);
    const auto totalRows = static_cast<std::size_t>(std::ranges::count(jsonl, '\n'));
    auto pool = NES::BufferManager::create(
        static_cast<std::uint32_t>(bufferSize),
        static_cast<std::uint32_t>(decodePoolBuffers(totalRows, rowsPerBuffer, varSizedFieldCount(schema))));
    Formatter formatter(schema, pool);
    auto buffers = formatter.decode(jsonl);
    return {.pool = std::move(pool), .buffers = std::move(buffers)};
}

}
