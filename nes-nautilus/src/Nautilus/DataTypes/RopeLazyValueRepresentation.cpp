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

#include <Nautilus/DataTypes/RopeLazyValueRepresentation.hpp>

#include <cstdint>
#include <utility>

#include <cstddef>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/std/cstring.h>
#include <Arena.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

RopeLazyValueRepresentation::RopeLazyValueRepresentation(
    std::vector<Span> spans,
    const nautilus::val<uint64_t>& totalSize,
    const nautilus::val<Arena*>& arena,
    const DataType& type,
    const nautilus::val<bool>& isNull)
    /// The base ptr is unused for a rope (there is no single backing buffer until materialised); getContent()
    /// and parseValue() are overridden to materialise. We still hand the base the correct size so getSize()
    /// (non-virtual, reads the member) reports the logical length without a walk.
    : LazyValueRepresentation(nautilus::val<int8_t*>(nullptr), totalSize, type, isNull, ""), spans(std::move(spans)), arena(arena)
{
}

void RopeLazyValueRepresentation::materialize() const
{
    if (materialized)
    {
        return;
    }
    /// Exactly the eager body CONCAT used to run, now deferred: allocate Σlen and copy each span in order.
    /// The span count is compile-time, so this loop is host-unrolled. The induction variable is a static_val
    /// so each unrolled iteration gets a distinct trace tag -- a plain loop counter would emit the copy from
    /// one source location every iteration, which the tracer would mistake for a loop back-edge.
    const ArenaRef arenaRef(arena);
    const auto destination = arenaRef.allocateVariableSizedData(getSize());
    nautilus::val<uint64_t> offset{0};
    for (nautilus::static_val<std::size_t> spanIndex = 0; spanIndex < spans.size(); ++spanIndex)
    {
        const std::size_t index = spanIndex;
        nautilus::memcpy(destination.getContent() + offset, spans[index].ptr, spans[index].len);
        offset += spans[index].len;
    }
    materializedData = destination;
    materialized = true;
}

nautilus::val<int8_t*> RopeLazyValueRepresentation::getContent() const
{
    materialize();
    return materializedData->getContent();
}

VarVal RopeLazyValueRepresentation::parseValue() const
{
    materialize();
    return VarVal{*materializedData, type.nullable, isNull};
}

}
