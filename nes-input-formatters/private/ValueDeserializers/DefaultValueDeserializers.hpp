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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/Strings.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>

namespace NES::DefaultValueDeserializer
{

/// Everything below is plain C++ behind a single nautilus call, which is why the quote trimming, the whitespace
/// trimming and the null check live here rather than at the call site.

/// We expect a pointer and the size so that we can use this method from the nautilus runtime
inline bool checkIsNull(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues)
{
    /// A field the RawBufferIndex could not locate arrives as {nullptr, 0}.
    if (fieldAddress == nullptr)
    {
        return true;
    }

    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    /// A view over the raw buffer, compared against the null literals without copying either side.
    const std::string_view fieldAsString{reinterpret_cast<const char*>(fieldAddress), fieldSize};
    return std::ranges::any_of(*nullValues, [&fieldAsString](const std::string& nullValue) { return nullValue == fieldAsString; });
}

/// Returns the field without its trailing whitespace.
inline std::string_view truncateTrailingSpaces(const int8_t* ptr, const uint64_t size)
{
    std::string_view uncutString{reinterpret_cast<const char*>(ptr), size};
    if (const auto lastNonWs = uncutString.find_last_not_of(" \t\n\r"); lastNonWs != std::string_view::npos)
    {
        uncutString = uncutString.substr(0, lastNonWs + 1);
    }
    return uncutString;
}

/// Parses one fixed-size field: null detection, quote stripping and the parse itself.
template <typename T, bool Nullable, bool Quoted>
DeserializedValue* deserializeFixedSized(int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues, Arena*)
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");

    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static DeserializedValue result;
    result.isNull = false;

    const auto writeValue = [](const T value) { std::memcpy(result.value.data(), &value, sizeof(T)); };

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNull(fieldAddress, fieldSize, nullValues))
        {
            result.isNull = true;
            writeValue(T{0});
            return &result;
        }
    }

    /// Remove the quotes from the address, if quoted.
    const int8_t* trueFieldAddress = Quoted ? fieldAddress + 1 : fieldAddress;
    const uint64_t trueFieldSize = Quoted ? fieldSize - 2 : fieldSize;

    try
    {
        /// trimWhiteSpaces() and from_chars_with_exception() both take a string_view, so the field is parsed
        /// straight out of the raw buffer without materialising a string.
        const std::string_view fieldAsString{reinterpret_cast<const char*>(trueFieldAddress), trueFieldSize};
        writeValue(NES::from_chars_with_exception<T>(trimWhiteSpaces(fieldAsString)));
    }
    catch (const Exception&)
    {
        /// If the field is nullable, we return a null value, otherwise we throw an exception
        if constexpr (not Nullable)
        {
            throw;
        }
        result.isNull = true;
        writeValue(T{0});
    }
    return &result;
}

/// Resolves a var-sized field to bounds inside the raw buffer; there is nothing to parse, only to trim.
/// The null check runs on the untrimmed bounds, so a quoted NULL literal still matches.
template <bool Nullable, bool Quoted, bool HasTrailingSpaces>
DeserializedValue* deserializeVarSized(int8_t* fieldAddress, uint64_t fieldSize, const std::vector<std::string>* nullValues, Arena*)
{
    thread_local static DeserializedValue result;
    result.isNull = false;

    if constexpr (Nullable)
    {
        if (checkIsNull(fieldAddress, fieldSize, nullValues))
        {
            result.varsizedPtr = nullptr;
            result.varsizedSize = 0;
            result.isNull = true;
            return &result;
        }
    }

    if constexpr (HasTrailingSpaces)
    {
        const auto truncated = truncateTrailingSpaces(fieldAddress, fieldSize);
        fieldAddress = const_cast<int8_t*>(reinterpret_cast<const int8_t*>(truncated.data())); /// NOLINT: points into the raw buffer
        fieldSize = truncated.size();
    }
    if constexpr (Quoted)
    {
        fieldAddress += 1;
        fieldSize -= 2;
    }

    result.varsizedPtr = fieldAddress;
    result.varsizedSize = fieldSize;
    return &result;
}

}

namespace NES
{

/// Covers every fixed-size default deserializer. Type and nullability are template parameters because they select
/// the parse; quoting arrives as a registry argument and picks between two instantiations.
template <typename T, bool Nullable>
class DefaultFixedSizeValueDeserializer final : public ValueDeserializer
{
public:
    DefaultFixedSizeValueDeserializer(const std::string_view typeKey, const bool quoted)
        : ValueDeserializer(composeName(typeKey, quoted)), quoted(quoted)
    {
    }

    [[nodiscard]] DeserializeProxy proxy() const override
    {
        return quoted ? &DefaultValueDeserializer::deserializeFixedSized<T, Nullable, true>
                      : &DefaultValueDeserializer::deserializeFixedSized<T, Nullable, false>;
    }

    [[nodiscard]] DataType::Type producedType() const override { return dataTypeOf<T>(); }

private:
    static std::string composeName(const std::string_view typeKey, const bool quoted)
    {
        return std::string{Nullable ? "Nullable" : ""}.append(typeKey).append(quoted ? "_Quoted" : "");
    }

    bool quoted;
};

/// Var-sized fields are referenced in place rather than parsed. Both flags select an instantiation, hence four.
template <bool Nullable>
class DefaultVarSizedValueDeserializer final : public ValueDeserializer
{
public:
    DefaultVarSizedValueDeserializer(const bool quoted, const bool hasTrailingSpaces)
        : ValueDeserializer(composeName(quoted, hasTrailingSpaces)), quoted(quoted), hasTrailingSpaces(hasTrailingSpaces)
    {
    }

    [[nodiscard]] DeserializeProxy proxy() const override
    {
        if (quoted)
        {
            return hasTrailingSpaces ? &DefaultValueDeserializer::deserializeVarSized<Nullable, true, true>
                                     : &DefaultValueDeserializer::deserializeVarSized<Nullable, true, false>;
        }
        return hasTrailingSpaces ? &DefaultValueDeserializer::deserializeVarSized<Nullable, false, true>
                                 : &DefaultValueDeserializer::deserializeVarSized<Nullable, false, false>;
    }

    [[nodiscard]] DataType::Type producedType() const override { return DataType::Type::VARSIZED; }

private:
    static std::string composeName(const bool quoted, const bool hasTrailingSpaces)
    {
        return std::string{Nullable ? "NullableDefaultVARSIZED" : "DefaultVARSIZED"}
            .append(quoted ? "_Quoted" : "")
            .append(hasTrailingSpaces ? "_Trailing" : "");
    }

    bool quoted;
    bool hasTrailingSpaces;
};
}
