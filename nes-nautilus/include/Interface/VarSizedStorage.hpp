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
#include <span>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <val.hpp>

/// Containers keeping VARSIZED payloads outside the record store a VariableSizedAccess in the field slot and the payload in a child
/// buffer. Only where that child buffer comes from differs, so each strategy is its own factory producing a LoadVarSized/StoreVarSized.
namespace NES
{

/// Copies varSizedValue into tupleBuffer's last child buffer if it fits, else attaches a fresh one, and returns where it landed.
VariableSizedAccess
writeVarSized(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, std::span<const std::byte> varSizedValue);

/// Resolves a VariableSizedAccess against tupleBuffer's child buffers.
std::span<std::byte> loadAssociatedVarSizedValue(const TupleBuffer& tupleBuffer, VariableSizedAccess variableSizedAccess) noexcept;

/// Reads a VariableSizedAccess slot against buffer's child buffers.
LoadVarSized makeVarSizedLoadFunction(const NautilusBuffer& buffer);

/// Appends to buffer's last child buffer if the payload fits, else attaches a new one.
StoreVarSized
makeChildBufferVarSizedStoreFunction(const NautilusBuffer& buffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider);

/// Appends to the varsized page tracked in the BTree header. A BTree cannot use the child-buffer strategy, as its child buffers
/// double as node pages and thus have no meaningful "last" element.
StoreVarSized makeBTreeVarSizedStoreFunction(const NautilusBuffer& buffer, const nautilus::val<AbstractBufferProvider*>& bufferProvider);

}
