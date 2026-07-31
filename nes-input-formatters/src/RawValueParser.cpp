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
#include <function.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

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
    nautilus::val<int8_t*> address;
    nautilus::val<uint64_t> size;
};

/// Loads the field bounds from the field-offset index buffer and turns them into the field's address and size.
RawField loadRawField(
    const nautilus::val<int8_t*>& recordBufferPtr,
    const nautilus::val<const FieldIndex*>& indexBufferPtr,
    const nautilus::val<uint64_t>& offsetIdx,
    const nautilus::val<uint64_t>& sizeOfDelimiter)
{
    const auto fieldOffsetStart = readValueFromMemRef<FieldIndex>(indexBufferPtr + offsetIdx);
    const auto fieldOffsetEnd = readValueFromMemRef<FieldIndex>(indexBufferPtr + offsetIdx + nautilus::val<uint64_t>(1));
    return {.address = recordBufferPtr + fieldOffsetStart, .size = fieldOffsetEnd - fieldOffsetStart - sizeOfDelimiter};
}

/// Parses a fixed-size field. The parse is traced once into a nautilus function shared by all like-typed fields, instead of
/// inlining it at every column -- that collapses the traced IR and cuts JIT compile time on wide schemas (196-UINT64 test).
/// The shared function also performs the offset-index loads and the field address/size computation, so per field only the
/// offset index computation and this call remain inline. A VarVal cannot cross the function boundary, so only its runtime
/// content does: non-nullable fields return the raw nautilus::val<T>; nullable fields return the proxy's ParseResult<T>*
/// (value + null flag), read back on the caller side.
template <typename T>
void writeFixedSizeField(
    CompilationContext& compilationContext,
    Record& record,
    const DataType& dataType,
    const nautilus::val<int8_t*>& recordBufferPtr,
    const nautilus::val<const FieldIndex*>& indexBufferPtr,
    const nautilus::val<uint64_t>& offsetIdx,
    const uint64_t sizeOfDelimiter,
    const QualifiedIdentifier& fieldName,
    const std::vector<std::string>& nullValues,
    const bool trimQuotes)
{
    /// Capture nullValues by pointer, not by copy: the proxy bakes &nullValues into the compiled code as a constant, so it
    /// must outlive the pipeline. A captured copy lives only in this lambda and would dangle once tracing is done.
    /// The type alone identifies the shared function because the other captures are constant within a pipeline: a pipeline
    /// has a single input formatter, so nullValues is one object and trimQuotes is set only for CHAR. Should a pipeline ever
    /// parse with two formatters, both would have to enter the name -- see registerTracedFunction().
    if (dataType.nullable)
    {
        using ParseSignature = nautilus::val<ParseResult<T>*>(
            nautilus::val<int8_t*>, nautilus::val<const FieldIndex*>, nautilus::val<uint64_t>, nautilus::val<uint64_t>);
        auto& parseFunction = compilationContext.registerTracedFunction<ParseSignature>(
            fmt::format("ParseRawValueNullable_{}", magic_enum::enum_name(dataType.type)),
            [nullValues = &nullValues, trimQuotes](
                const nautilus::val<int8_t*>& recordBuffer,
                const nautilus::val<const FieldIndex*>& indexBuffer,
                const nautilus::val<uint64_t>& offsetIndex,
                const nautilus::val<uint64_t>& delimiterSize)
            {
                auto [address, size] = loadRawField(recordBuffer, indexBuffer, offsetIndex, delimiterSize);
                if (trimQuotes)
                {
                    address = address + nautilus::val<uint32_t>(1);
                    size = size - nautilus::val<uint32_t>(2);
                }
                return nautilus::invoke(
                    parseIntoVarValProxy<T, true>, address, size, nautilus::val<const std::vector<std::string>*>{nullValues});
            });
        const auto parseResult = parseFunction(recordBufferPtr, indexBufferPtr, offsetIdx, nautilus::val<uint64_t>{sizeOfDelimiter});
        const nautilus::val<T> value = *getMemberWithOffset<T>(parseResult, offsetof(ParseResult<T>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResult<T>, isNull));
        record.write(fieldName, VarVal{value, true, isNull});
        return;
    }

    using ParseSignature
        = nautilus::val<T>(nautilus::val<int8_t*>, nautilus::val<const FieldIndex*>, nautilus::val<uint64_t>, nautilus::val<uint64_t>);
    auto& parseFunction = compilationContext.registerTracedFunction<ParseSignature>(
        fmt::format("ParseRawValue_{}", magic_enum::enum_name(dataType.type)),
        [nullValues = &nullValues, trimQuotes](
            const nautilus::val<int8_t*>& recordBuffer,
            const nautilus::val<const FieldIndex*>& indexBuffer,
            const nautilus::val<uint64_t>& offsetIndex,
            const nautilus::val<uint64_t>& delimiterSize)
        {
            auto [address, size] = loadRawField(recordBuffer, indexBuffer, offsetIndex, delimiterSize);
            if (trimQuotes)
            {
                address = address + nautilus::val<uint32_t>(1);
                size = size - nautilus::val<uint32_t>(2);
            }
            return parseFixedSizeIntoVarVal<T>(false, address, size, *nullValues).template getRawValueAs<nautilus::val<T>>();
        });
    record.write(
        fieldName,
        VarVal{
            parseFunction(recordBufferPtr, indexBufferPtr, offsetIdx, nautilus::val<uint64_t>{sizeOfDelimiter}),
            false,
            nautilus::val<bool>{false}});
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
    /// Type dispatch: the lambda binds the shared arguments once, so each case below only selects the C++ type.
    /// Without it, every case would repeat the full writeFixedSizeField call with all ten arguments.
    const auto parseFixedSizeField = [&]<typename T>(const bool trimQuotes = false)
    {
        writeFixedSizeField<T>(
            compilationContext,
            record,
            dataType,
            recordBufferPtr,
            indexBufferPtr,
            offsetIdx,
            sizeOfDelimiter,
            fieldName,
            nullValues,
            trimQuotes);
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
            /// Varsized fields stay inline: the record references the raw buffer directly, so there is no compact
            /// runtime value that could travel back across a shared function boundary.
            const auto [fieldAddress, fieldSize]
                = loadRawField(recordBufferPtr, indexBufferPtr, offsetIdx, nautilus::val<uint64_t>{sizeOfDelimiter});
            nautilus::val<bool> isNull = false;
            if (dataType.nullable)
            {
                isNull = nautilus::invoke(
                    {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
                    checkIsNullProxy,
                    fieldAddress,
                    fieldSize,
                    nautilus::val<const std::vector<std::string>*>{&nullValues});
            }

            switch (quotationType)
            {
                case QuotationType::NONE: {
                    const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress);
                    const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize);
                    const VariableSizedData varSized{ptr, size};
                    const VarVal varVal{varSized, dataType.nullable, isNull};
                    record.write(fieldName, varVal);
                    return;
                }
                case QuotationType::DOUBLE_QUOTE: {
                    const auto ptr = nautilus::select(isNull, nautilus::val<int8_t*>{nullptr}, fieldAddress + nautilus::val<uint32_t>(1));
                    const auto size = nautilus::select(isNull, nautilus::val<uint64_t>{0}, fieldSize - nautilus::val<uint64_t>(2));
                    const VariableSizedData varSized{ptr, size};
                    const VarVal varVal{varSized, dataType.nullable, isNull};
                    record.write(fieldName, varVal);
                    return;
                }
            }
            std::unreachable();
        }
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}

}
