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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>
#include <SequenceShredder.hpp>

namespace NES::InputFormatters
{

/// Implementation detail of CSVInputFormatter
class ProgressTracker;

/// TODO #679: Major refactoring of CSVInputFormatter (see issue description for details)
class CSVInputFormatter final : public InputFormatter
{
private:
    enum class FormattedTupleIs : uint8_t
    {
        EMTPY = 0,
        NOT_EMPTY = 1,
    };

public:
    using CastFunctionSignature = std::function<void(
        std::string inputString,
        int8_t* fieldPointer,
        Memory::AbstractBufferProvider& bufferProvider,
        Memory::TupleBuffer& tupleBufferFormatted)>;

    CSVInputFormatter(const Schema& schema, std::string tupleDelimiter, std::string fieldDelimiter);
    ~CSVInputFormatter() override; /// needs implementation in source file to allow for forward declaring 'InputFormatter'

    CSVInputFormatter(const CSVInputFormatter&) = delete;
    CSVInputFormatter& operator=(const CSVInputFormatter&) = delete;
    CSVInputFormatter(CSVInputFormatter&&) = delete;
    CSVInputFormatter& operator=(CSVInputFormatter&&) = delete;

    void parseTupleBufferRaw(
        const Memory::TupleBuffer& rawTB,
        PipelineExecutionContext& pipelineExecutionContext,
        size_t numBytesInRawTB,
        SequenceShredder& sequenceShredder) override;

    /// Currently allocates new buffer if there is a trailing tuple. Would require keeping state between potentially different threads otherwise,
    /// since the stop call of the InputFormatterTask (pipeline) triggers the flush call.
    void
    flushFinalTuple(OriginId originId, PipelineExecutionContext& pipelineExecutionContext, SequenceShredder& sequenceShredder) override;

    size_t getSizeOfTupleDelimiter() override;
    size_t getSizeOfFieldDelimiter() override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    const Schema schema;
    const std::string fieldDelimiter;
    const std::string tupleDelimiter;
    std::vector<size_t> fieldSizes;
    std::vector<CastFunctionSignature> fieldParseFunctions;

    FormattedTupleIs processPartialTuple(
        const size_t partialTupleStartIdx,
        const size_t partialTupleEndIdx,
        const std::vector<SequenceShredder::StagedBuffer>& buffersToFormat,
        ProgressTracker& progressTracker,
        PipelineExecutionContext& pipelineExecutionContext);
};

}
