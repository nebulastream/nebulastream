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

/// Opt-in, allocation-free integer parsers backed by fast_float's integer
/// `from_chars` (the same Lemire lineage simdjson uses). They take the field's
/// (address, length) directly -- no end-scan needed -- and skip the Default
/// parsers' whitespace trim and `0x` hex detection for a lean hot loop, but keep
/// a full-consumption guard so a partial parse like "12x" is rejected. These are
/// opt-in per field via the `<WIDTH>_PARSER` descriptor override (e.g.
/// `"FastINT32" AS \`INPUT_FORMATTER\`.INT32_PARSER`); the Default parsers stay
/// the trim/hex-tolerant fallback.
namespace NES
{
class FastINT8InputParser final : public InputParser
{
public:
    explicit FastINT8InputParser() noexcept = default;
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

class FastINT16InputParser final : public InputParser
{
public:
    explicit FastINT16InputParser() noexcept = default;
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

class FastINT32InputParser final : public InputParser
{
public:
    explicit FastINT32InputParser() noexcept = default;
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

class FastINT64InputParser final : public InputParser
{
public:
    explicit FastINT64InputParser() noexcept = default;
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

class FastUINT8InputParser final : public InputParser
{
public:
    explicit FastUINT8InputParser() noexcept = default;
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

class FastUINT16InputParser final : public InputParser
{
public:
    explicit FastUINT16InputParser() noexcept = default;
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

class FastUINT32InputParser final : public InputParser
{
public:
    explicit FastUINT32InputParser() noexcept = default;
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

class FastUINT64InputParser final : public InputParser
{
public:
    explicit FastUINT64InputParser() noexcept = default;
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
