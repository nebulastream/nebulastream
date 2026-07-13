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

/// Opt-in integer parsers whose digit logic is emitted as TRACED nautilus ops -- the same
/// fast_float/simdjson SWAR algorithm (parse eight digits at once via 64-bit integer arithmetic),
/// but written directly in `parseToVarVal` instead of behind a `NAUTILUS_TAGGED_INVOKE`. Because
/// `parseToVarVal` is trace-time codegen (its `val<>` args only exist while the pipeline is built),
/// the emitted ops FUSE into the JIT'd scan loop -- no per-field proxy call. This matters on targets
/// where the nautilus InliningPass is unavailable (e.g. ARM): the FastInteger plugin's fast_float
/// invoke stays an un-inlined call there, whereas these inline on every platform.
///
/// Opt-in per field via the `<WIDTH>_PARSER` descriptor override (e.g. `"TracedUINT64" AS
/// \`INPUT_FORMATTER\`.UINT64_PARSER`). Non-nullable fields take the lean value-only path (the
/// digit-validity check is dead-code-eliminated). For nullable fields a non-digit / overflow sets
/// isNull. KNOWN LIMITATIONS (use Default/Fast if needed): (1) traced code cannot throw, so a
/// malformed NON-nullable field is parsed best-effort rather than raising; (2) configured null-marker
/// strings are not matched (only digit-validity drives isNull). Owes a correctness systest across all
/// 8 types (min/max, negatives, narrow-overflow, non-digit) before production -- same as FastInteger.
namespace NES
{
class TracedINT8InputParser final : public InputParser
{
public:
    explicit TracedINT8InputParser() noexcept = default;
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

class TracedINT16InputParser final : public InputParser
{
public:
    explicit TracedINT16InputParser() noexcept = default;
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

class TracedINT32InputParser final : public InputParser
{
public:
    explicit TracedINT32InputParser() noexcept = default;
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

class TracedINT64InputParser final : public InputParser
{
public:
    explicit TracedINT64InputParser() noexcept = default;
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

class TracedUINT8InputParser final : public InputParser
{
public:
    explicit TracedUINT8InputParser() noexcept = default;
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

class TracedUINT16InputParser final : public InputParser
{
public:
    explicit TracedUINT16InputParser() noexcept = default;
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

class TracedUINT32InputParser final : public InputParser
{
public:
    explicit TracedUINT32InputParser() noexcept = default;
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

class TracedUINT64InputParser final : public InputParser
{
public:
    explicit TracedUINT64InputParser() noexcept = default;
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

/// Booleans decide on the FIRST byte only: 't'/'T'/'1' -> true, 'f'/'F'/'0' -> false (matches the
/// Default parser's accepted set {true,false,1,0} case-insensitively for well-formed fields; the
/// token tail is not validated -- same best-effort contract as the traced integer parsers).
class TracedBOOLInputParser final : public InputParser
{
public:
    explicit TracedBOOLInputParser() noexcept = default;
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
