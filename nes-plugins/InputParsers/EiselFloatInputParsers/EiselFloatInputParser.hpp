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
#include <string>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <InputParser.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

/// Opt-in, allocation-free float parsers with a hand-rolled fast_float
/// (Eisel-Lemire) algorithm: parse the ASCII digits into a 64-bit mantissa plus a
/// decimal exponent, then turn `mantissa * 10^exponent` into the correctly-rounded
/// IEEE-754 value via a 128-bit power-of-five multiply (see fast_float
/// `decimal_to_binary.h`). No standard library, no `from_chars` -- unlike the
/// `Fast*` parsers, which wrap `fast_float::from_chars`. The algorithm is ordinary
/// C++ in a proxy function that nautilus inlines into the JIT'd scan loop.
///
/// This is the *lean* variant: it has no big-integer decimal slow path, so the
/// few inputs with more than 19 significant digits whose rounding the Lemire step
/// cannot resolve exactly are treated as malformed (non-nullable -> throw,
/// nullable -> null). `inf`/`nan`/hex floats are likewise out of scope. Opt-in per
/// field via the `<WIDTH>_PARSER` descriptor override (e.g. `"EiselFloatF64" AS
/// \`INPUT_FORMATTER\`.F64_PARSER`).
namespace NES
{
class EiselFloatF32InputParser final : public InputParser
{
public:
    explicit EiselFloatF32InputParser() noexcept = default;
    [[nodiscard]] VarVal parseToVarVal(
        bool nullable,
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;

    [[nodiscard]] VarVal parseLazyToVarVal(
        const bool& nullable,
        const nautilus::val<bool>& isNull,
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize) const override;
};

class EiselFloatF64InputParser final : public InputParser
{
public:
    explicit EiselFloatF64InputParser() noexcept = default;
    [[nodiscard]] VarVal parseToVarVal(
        bool nullable,
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;

    [[nodiscard]] VarVal parseLazyToVarVal(
        const bool& nullable,
        const nautilus::val<bool>& isNull,
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize) const override;
};
}
