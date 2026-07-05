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

/// Opt-in float parsers whose COMMON-CASE digit logic is emitted as TRACED nautilus ops -- the same
/// idea as the TracedInteger parsers, applied to floats. A plain decimal `[sign] int [ '.' frac ]`
/// (no exponent, <= 15 significant digits for double / 7 for float, <= 22 / 10 fractional digits) is
/// parsed entirely with `val<>` trace-time arithmetic: accumulate the mantissa, count fractional
/// digits, then one Clinger fast-path scale `mantissa / 10^fracDigits` against a tiny (23 / 11 entry)
/// power-of-ten table read via `readValueFromMemRef`. Because that all lives in `parseToVarVal` it
/// FUSES into the JIT'd scan loop -- no per-field proxy call, unlike the Eisel/Fast float parsers whose
/// bodies stay a native call (they showed up as `parseWithEiselFloat`/`parseNumberString` in the
/// sequential profile). q1's integer-valued prices (exp 0) hit this fast path.
///
/// Anything the fast path cannot represent EXACTLY -- exponent notation (`1e5`), > 15/7 digits, a second
/// '.', any other byte, or an empty field -- sets a `hard` flag and the parse falls back to a proxy
/// `fast_float::from_chars` call (same algorithm class as Eisel, so the result is bit-identical). The
/// Clinger fast path is itself bit-identical to Eisel's `tryFastPath`, so a Checksum A/B vs Eisel on
/// plain-decimal data matches exactly. `inf`/`nan`/hex go through the fallback.
///
/// Opt-in per field via the `<WIDTH>_PARSER` descriptor override (e.g. `"TracedFloatF64" AS
/// \`INPUT_FORMATTER\`.F64_PARSER`). The nullable path delegates to the proxy (configured null markers
/// are matched there); the non-nullable path is the fully-traced fast path + proxy fallback.
namespace NES
{
class TracedFloatF32InputParser final : public InputParser
{
public:
    explicit TracedFloatF32InputParser() noexcept = default;
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

class TracedFloatF64InputParser final : public InputParser
{
public:
    explicit TracedFloatF64InputParser() noexcept = default;
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
