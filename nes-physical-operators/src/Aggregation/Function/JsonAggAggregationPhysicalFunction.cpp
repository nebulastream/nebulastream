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

#include <Aggregation/Function/JsonAggAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/JsonAggEncoding.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/format.h>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

void JsonArrayBuilder::append(const std::string_view value)
{
    if (!empty)
    {
        text.push_back(',');
    }
    empty = false;
    text.push_back('"');
    for (const char character : value)
    {
        switch (character)
        {
            case '"':
                text += R"(\")";
                break;
            case '\\':
                text += R"(\\)";
                break;
            case '\b':
                text += R"(\b)";
                break;
            case '\f':
                text += R"(\f)";
                break;
            case '\n':
                text += R"(\n)";
                break;
            case '\r':
                text += R"(\r)";
                break;
            case '\t':
                text += R"(\t)";
                break;
            default:
                if (static_cast<unsigned char>(character) < 0x20)
                {
                    text += fmt::format("\\u{:04x}", static_cast<unsigned>(character));
                }
                else
                {
                    text.push_back(character);
                }
        }
    }
    text.push_back('"');
}

const std::string& JsonArrayBuilder::finish()
{
    if (!finished)
    {
        text.push_back(']');
        finished = true;
    }
    return text;
}

namespace
{
/// Loads the window's paged vector from the parent buffer through the child index stored in the state.
OwnedNautilusBuffer loadPagedVector(const nautilus::val<AggregationState*>& aggregationState, BorrowedNautilusBuffer& parentBuffer)
{
    OwnedNautilusBuffer pagedVecBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        pagedVecBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)));
    return pagedVecBuffer;
}
}

JsonAggAggregationPhysicalFunction::JsonAggAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
{
}

void JsonAggAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    const auto pagedVecBuffer = loadPagedVector(aggregationState, parentBuffer);
    PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
    pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
}

void JsonAggAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// The second state's records go after the first's, so the array order is the order the slices are combined in.
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* parent1,
            const uint32_t* indexPtr1,
            TupleBuffer* parent2,
            const uint32_t* indexPtr2) -> void
        {
            const TupleBuffer vec1Buf = parent1->loadChildBuffer(ChildBufferIndex{*indexPtr1});
            const TupleBuffer vec2Buf = parent2->loadChildBuffer(ChildBufferIndex{*indexPtr2});
            auto vector1 = PagedVector::load(vec1Buf);
            const auto vector2 = PagedVector::load(vec2Buf);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        parentBuffer1.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState1)),
        parentBuffer2.asArg(),
        static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState2)));
}

Record JsonAggAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto pagedVecBuffer = loadPagedVector(aggregationState, parentBuffer);
    const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);

    /// The text is built on the heap by plain C++, then copied into the arena, which outlives the emit of this record.
    const auto builder = nautilus::invoke(+[]() { return new JsonArrayBuilder(); }); /// NOLINT(cppcoreguidelines-owning-memory)
    for (const auto& itemRecord : pagedVectorRef)
    {
        const auto value = inputFunction.execute(itemRecord, pipelineMemoryProvider.arena).getRawValueAs<VariableSizedData>();
        nautilus::invoke(
            +[](JsonArrayBuilder* json, const int8_t* content, const uint64_t size)
            {
                json->append(std::string_view{reinterpret_cast<const char*>(content), size});
            }, /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            builder,
            value.getContent(),
            value.getSize());
    }
    const auto size = nautilus::invoke(+[](JsonArrayBuilder* json) -> uint64_t { return json->finish().size(); }, builder);
    auto output = pipelineMemoryProvider.arena.allocateVariableSizedData(size);
    nautilus::invoke(
        +[](JsonArrayBuilder* json, int8_t* out)
        {
            const auto& text = json->finish();
            /// A VARSIZED value carries its size, not a terminator.
            std::memcpy(out, text.data(), text.size()); /// NOLINT(bugprone-not-null-terminated-result)
            delete json; /// NOLINT(cppcoreguidelines-owning-memory)
        },
        builder,
        output.getContent());

    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VariableSizedData(output.getContent(), size));
    return resultRecord;
}

void JsonAggAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = getSizeInBytes(tupleLayout->getSchema());
    const nautilus::val<uint32_t> childBufferIndexVal = nautilus::invoke(
        +[](TupleBuffer* parentBuffer, AbstractBufferProvider* bufferProvider, uint64_t tupleSize)
        {
            if (auto pagedVectorBufferOpt = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                auto pagedVectorBuffer = pagedVectorBufferOpt.value();
                PagedVector::init(pagedVectorBuffer, bufferProvider->getBufferSize(), tupleSize);
                return parentBuffer->storeChildBuffer(pagedVectorBuffer).getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for the JSON_AGG paged vector!");
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
    *static_cast<nautilus::val<uint32_t*>>(static_cast<nautilus::val<int8_t*>>(aggregationState)) = childBufferIndexVal;
}

void JsonAggAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// No-op: the paged vector is a child of the parent hash map buffer and is released with it.
}

size_t JsonAggAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// The paged vector's child buffer index.
    return sizeof(uint32_t);
}

AggregationPhysicalFunctionRegistryReturnType
JsonAggAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<JsonAggAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}

}
