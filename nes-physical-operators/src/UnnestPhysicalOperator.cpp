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


#include <UnnestPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <UnnestDecoding.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

UnnestPhysicalOperator::UnnestPhysicalOperator(Record::RecordFieldIdentifier input, Record::RecordFieldIdentifier output)
    : input(std::move(input)), output(std::move(output))
{
}

namespace
{
/// The decoded elements live on the heap only between the two calls below: the first decodes and sizes them (and throws
/// for anything that is not a JSON array, before anything is allocated), the second packs them into the arena and frees
/// them.
std::vector<std::string>* decode(const int8_t* content, const uint64_t size)
{
    return new std::vector<std::string>(decodeJsonArrayElements( /// NOLINT(cppcoreguidelines-owning-memory)
        std::string_view{reinterpret_cast<const char*>(content), size})); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

uint64_t sizeOf(const std::vector<std::string>* elements)
{
    return packedElementsSize(*elements);
}

void packAndFree(std::vector<std::string>* elements, int8_t* block, const uint64_t size)
{
    packElements(*elements, std::span{reinterpret_cast<std::byte*>(block), size}); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    delete elements; /// NOLINT(cppcoreguidelines-owning-memory)
}

uint64_t countOf(const int8_t* block)
{
    return packedCount(reinterpret_cast<const std::byte*>(block)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

int8_t* elementOf(int8_t* block, const uint64_t count, const uint64_t index)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-type-const-cast)
    return const_cast<int8_t*>(reinterpret_cast<const int8_t*>(packedElement(reinterpret_cast<const std::byte*>(block), count, index)));
}

uint64_t elementSizeOf(const int8_t* block, const uint64_t index)
{
    return packedElementSize(reinterpret_cast<const std::byte*>(block), index); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}
}

void UnnestPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto array = record.read(input).getRawValueAs<VariableSizedData>();
    const auto elements = nautilus::invoke(decode, array.getContent(), array.getSize());
    const auto blockSize = nautilus::invoke(sizeOf, elements);
    const auto block = ctx.pipelineMemoryProvider.arena.allocateMemory(blockSize);
    nautilus::invoke(packAndFree, elements, block, blockSize);

    const auto count = nautilus::invoke(countOf, block);
    for (nautilus::val<uint64_t> index = 0; index < count; index = index + 1)
    {
        const auto content = nautilus::invoke(elementOf, block, count, index);
        const auto size = nautilus::invoke(elementSizeOf, block, index);
        Record element = record;
        element.write(output, VariableSizedData(content, size));
        executeChild(ctx, element);
    }
}

std::optional<PhysicalOperator> UnnestPhysicalOperator::getChild() const
{
    return child;
}

void UnnestPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
