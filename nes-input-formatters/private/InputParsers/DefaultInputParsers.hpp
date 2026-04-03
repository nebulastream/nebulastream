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

namespace NES
{
class DefaultBOOLInputParser final : public InputParser
{
public:
    explicit DefaultBOOLInputParser() noexcept = default;
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

class DefaultCHARInputParser final : public InputParser
{
public:
    explicit DefaultCHARInputParser() noexcept = default;
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

class DefaultF32InputParser final : public InputParser
{
public:
    explicit DefaultF32InputParser() noexcept = default;
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

class DefaultF64InputParser final : public InputParser
{
public:
    explicit DefaultF64InputParser() noexcept = default;
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

class DefaultINT8InputParser final : public InputParser
{
public:
    explicit DefaultINT8InputParser() noexcept = default;
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

class DefaultINT16InputParser final : public InputParser
{
public:
    explicit DefaultINT16InputParser() noexcept = default;
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

class DefaultINT32InputParser final : public InputParser
{
public:
    explicit DefaultINT32InputParser() noexcept = default;
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

class DefaultINT64InputParser final : public InputParser
{
public:
    explicit DefaultINT64InputParser() noexcept = default;
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

class DefaultUINT8InputParser final : public InputParser
{
public:
    explicit DefaultUINT8InputParser() noexcept = default;
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

class DefaultUINT16InputParser final : public InputParser
{
public:
    explicit DefaultUINT16InputParser() noexcept = default;
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

class DefaultUINT32InputParser final : public InputParser
{
public:
    explicit DefaultUINT32InputParser() noexcept = default;
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

class DefaultUINT64InputParser final : public InputParser
{
public:
    explicit DefaultUINT64InputParser() noexcept = default;
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
