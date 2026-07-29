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
#include <cstdint>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputParser.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Serialises floats via the xjb ftoa algorithm (https://github.com/xjb714/xjb, Apache-2.0).
/// A shortest-round-trip float-to-string competitor to ZMIJ / std::to_chars.
class XJBF32OutputParser final : public OutputParser
{
public:
    explicit XJBF32OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    /// xjb requires a >= 21-byte buffer for a float (max effective 15 digits).
    [[nodiscard]] uint64_t maxOutputWidth() const override { return 21; }
};

class XJBF64OutputParser final : public OutputParser
{
public:
    explicit XJBF64OutputParser() noexcept = default;
    [[nodiscard]] nautilus::val<uint64_t> parseAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override;

    /// xjb requires a >= 34-byte buffer for a double (max effective 24 digits).
    [[nodiscard]] uint64_t maxOutputWidth() const override { return 34; }
};
}
