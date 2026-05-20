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
#include <DataTypes/VarVal.hpp>
#include <ValueDeserializer.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
class DefaultBOOLValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultBOOLValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultCHARValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultCHARValueDeserializer(const bool quoted) noexcept : quoted(quoted) { }

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;

private:
    bool quoted;
};

class DefaultF32ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultF32ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultF64ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultF64ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultINT8ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultINT8ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultINT16ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultINT16ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultINT32ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultINT32ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultINT64ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultINT64ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultUINT8ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultUINT8ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultUINT16ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultUINT16ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultUINT32ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultUINT32ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultUINT64ValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultUINT64ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class DefaultVARSIZEDValueDeserializer final : public ValueDeserializer
{
public:
    explicit DefaultVARSIZEDValueDeserializer(const bool& quoted, const bool& hasTrailingSpaces) noexcept
        : quoted(quoted), hasTrailingSpaces(hasTrailingSpaces)
    {
    }

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;

private:
    bool quoted;
    bool hasTrailingSpaces;
};

class NullableDefaultBOOLValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultBOOLValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultCHARValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultCHARValueDeserializer(const bool& quoted) noexcept : quoted(quoted) { }

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;

private:
    bool quoted;
};

class NullableDefaultF32ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultF32ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultF64ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultF64ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultINT8ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultINT8ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultINT16ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultINT16ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultINT32ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultINT32ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultINT64ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultINT64ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultUINT8ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultUINT8ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultUINT16ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultUINT16ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultUINT32ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultUINT32ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultUINT64ValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultUINT64ValueDeserializer() noexcept = default;
    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;
};

class NullableDefaultVARSIZEDValueDeserializer final : public ValueDeserializer
{
public:
    explicit NullableDefaultVARSIZEDValueDeserializer(const bool& quoted, const bool hasTrailingSpaces) noexcept
        : quoted(quoted), hasTrailingSpaces(hasTrailingSpaces)
    {
    }

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues) const override;

private:
    bool quoted;
    bool hasTrailingSpaces;
};
}
