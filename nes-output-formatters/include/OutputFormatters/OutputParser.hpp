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

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES
{

/// Base class for all output parser implementations
class OutputParser
{
public:
    OutputParser() noexcept = default;
    virtual ~OutputParser() noexcept = default;

    /// Parse the VarVal to a string and write it into the record buffer
    virtual nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const
        = 0;

    /// A true UPPER BOUND (in bytes) on what parseAndWrite can emit for a value of this parser's type.
    /// An output formatter that serialises a computed value directly into a pre-reserved main-buffer slot
    /// (its whole-record fast path) uses this to size the reservation; reserving less than the parser can
    /// emit risks an over-write, so the value MUST bound every output the parser produces. The default is a
    /// conservative bound covering the widest numeric text -- a fixed-notation double is ~317 chars, 344
    /// bounds it. Parsers with a tighter guarantee override this (e.g. ZMIJ's shortest round-trip float is
    /// <= 25) so float-heavy schemas still fit small buffers.
    [[nodiscard]] virtual uint64_t maxOutputWidth() const { return 344; }
};
}
