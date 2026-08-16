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

#include <RawValueParser.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <std/cstring.h>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <RawTupleBuffer.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
bool checkIsNullProxy(const int8_t* fieldAddress, const uint64_t fieldSize, const std::vector<std::string>* nullValues) noexcept
{
    PRECONDITION(nullValues != nullptr, "NullValues is expected to be not null!");
    const std::string fieldAsString{fieldAddress, fieldAddress + fieldSize};
    return std::ranges::any_of(*nullValues, [fieldAsString](const std::string& nullValue) { return nullValue == fieldAsString; });
}

namespace
{
struct RawField
{
    int8_t* address;
    uint64_t size;
    bool isNull;
};

/// Loads the field bounds from the field-offset index buffer and turns them into the field's address and size.
/// Plain C++ on purpose: called from the proxies below, it costs no trace operations there.
RawField loadRawField(int8_t* recordBuffer, const FieldIndex* indexBuffer, const uint64_t offsetIdx, const uint64_t delimiterSize)
{
    const FieldIndex fieldOffsetStart = indexBuffer[offsetIdx];
    const FieldIndex fieldOffsetEnd = indexBuffer[offsetIdx + 1];
    return RawField{.address = recordBuffer + fieldOffsetStart, .size = fieldOffsetEnd - fieldOffsetStart - delimiterSize, .isNull = false};
}

/// Parses the fixed-size field at the given offset index. Everything -- the offset-index loads, the quote trimming and the
/// parse itself -- happens in this one runtime function, so per field only the offset index computation and a single call
/// remain in the trace.
template <typename T, bool Nullable>
ParseResult<T>* parseRawFieldProxy(
    int8_t* recordBuffer,
    const FieldIndex* indexBuffer,
    const uint64_t offsetIdx,
    const uint64_t delimiterSize,
    const bool trimQuotes,
    const std::vector<std::string>* nullValues)
{
    auto field = loadRawField(recordBuffer, indexBuffer, offsetIdx, delimiterSize);
    if (trimQuotes)
    {
        field.address += 1;
        field.size -= 2;
    }
    return parseIntoVarValProxy<T, Nullable>(field.address, field.size, nullValues);
}

/// Resolves the bounds of a var-sized field, which the record references directly instead of parsing. A NULL field
/// resolves to an empty field. The null check runs on the untrimmed bounds, so a quoted NULL literal still matches.
template <bool Nullable>
RawField* loadVarSizedFieldProxy(
    int8_t* recordBuffer,
    const FieldIndex* indexBuffer,
    const uint64_t offsetIdx,
    const uint64_t delimiterSize,
    const bool trimQuotes,
    [[maybe_unused]] const std::vector<std::string>* nullValues)
{
    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static RawField result;
    result = loadRawField(recordBuffer, indexBuffer, offsetIdx, delimiterSize);
    if constexpr (Nullable)
    {
        if (checkIsNullProxy(result.address, result.size, nullValues))
        {
            result = RawField{.address = nullptr, .size = 0, .isNull = true};
            return &result;
        }
    }
    if (trimQuotes)
    {
        result.address += 1;
        result.size -= 2;
    }
    return &result;
}
}

void parseRawValueIntoRecord(
    CompilationContext& compilationContext,
    const DataType dataType,
    Record& record,
    const nautilus::val<int8_t*>& recordBufferPtr,
    const nautilus::val<const FieldIndex*>& indexBufferPtr,
    const nautilus::val<uint64_t>& offsetIdx,
    const uint64_t sizeOfDelimiter,
    const QualifiedIdentifier& fieldName,
    const std::vector<std::string>& nullValues,
    const QuotationType quotationType)
{
    /// The proxy arguments are the same for every field, so bind them once here.
    /// nullValues travels as an argument because the proxy bakes the pointer into the compiled code, which therefore
    /// has to outlive the pipeline -- the input formatter's member does.
    const auto callProxy = [&](auto& parseFunction, const bool trimQuotes)
    {
        return parseFunction(
            recordBufferPtr,
            indexBufferPtr,
            offsetIdx,
            nautilus::val<uint64_t>{sizeOfDelimiter},
            nautilus::val<bool>{trimQuotes},
            nautilus::val<const std::vector<std::string>*>{&nullValues});
    };

    /// Type dispatch: the lambda binds the shared arguments once, so each case below only selects the C++ type.
    /// One nautilus function per type and nullability, shared by all like-typed fields instead of inlining the parse at
    /// every column -- that collapses the traced IR and cuts JIT compile time on wide schemas (196-UINT64 test).
    /// Both registrations have the same signature and hence the same type, so the ternary picks one of the two.
    const auto parseFixedSizeField = [&]<typename T>(const bool trimQuotes = false)
    {
        auto& parseFunction = dataType.nullable
            ? compilationContext.registerTracedInvoke(
                  fmt::format("ParseRawValueNullable_{}", magic_enum::enum_name(dataType.type)), parseRawFieldProxy<T, true>)
            : compilationContext.registerTracedInvoke(
                  fmt::format("ParseRawValue_{}", magic_enum::enum_name(dataType.type)), parseRawFieldProxy<T, false>);
        const auto parseResult = callProxy(parseFunction, trimQuotes);
        const nautilus::val<T> value = *getMemberWithOffset<T>(parseResult, offsetof(ParseResult<T>, value));
        nautilus::val<bool> isNull{false};
        if (dataType.nullable)
        {
            isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<T>, isNull));
        }
        record.write(fieldName, VarVal{value, dataType.nullable, isNull});
    };
    switch (dataType.type)
    {
        case DataType::Type::BOOLEAN:
            parseFixedSizeField.operator()<bool>();
            return;
        case DataType::Type::INT8:
            parseFixedSizeField.operator()<int8_t>();
            return;
        case DataType::Type::INT16:
            parseFixedSizeField.operator()<int16_t>();
            return;
        case DataType::Type::INT32:
            parseFixedSizeField.operator()<int32_t>();
            return;
        case DataType::Type::INT64:
            parseFixedSizeField.operator()<int64_t>();
            return;
        case DataType::Type::UINT8:
            parseFixedSizeField.operator()<uint8_t>();
            return;
        case DataType::Type::UINT16:
            parseFixedSizeField.operator()<uint16_t>();
            return;
        case DataType::Type::UINT32:
            parseFixedSizeField.operator()<uint32_t>();
            return;
        case DataType::Type::UINT64:
            parseFixedSizeField.operator()<uint64_t>();
            return;
        case DataType::Type::FLOAT32:
            parseFixedSizeField.operator()<float>();
            return;
        case DataType::Type::FLOAT64:
            parseFixedSizeField.operator()<double>();
            return;
        case DataType::Type::CHAR:
            /// Quotation only shifts the field bounds; the shared char parse body is identical either way.
            parseFixedSizeField.operator()<char>(quotationType == QuotationType::DOUBLE_QUOTE);
            return;
        case DataType::Type::VARSIZED: {
            /// Var-sized fields go through the same shape: the record references the raw buffer directly, so the shared
            /// function hands back the field bounds instead of a parsed value.
            auto& loadFunction = dataType.nullable
                ? compilationContext.registerTracedInvoke("LoadVarSizedFieldNullable", loadVarSizedFieldProxy<true>)
                : compilationContext.registerTracedInvoke("LoadVarSizedField", loadVarSizedFieldProxy<false>);
            const auto rawField = callProxy(loadFunction, quotationType == QuotationType::DOUBLE_QUOTE);
            const VariableSizedData varSized{
                *getMemberWithOffset<int8_t*>(rawField, offsetof(RawField, address)),
                *getMemberWithOffset<uint64_t>(rawField, offsetof(RawField, size))};
            nautilus::val<bool> isNull{false};
            if (dataType.nullable)
            {
                isNull = *getMemberWithOffset<bool>(rawField, offsetof(RawField, isNull));
            }
            record.write(fieldName, VarVal{varSized, dataType.nullable, isNull});
            return;
        }
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}

}
