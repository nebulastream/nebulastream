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
#include <DataStructureTestUtils.hpp>

#include <any>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Record.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>
#include <options.hpp>

namespace NES::TestUtils
{

/// NOLINTBEGIN(misc-include-cleaner, bugprone-unchecked-optional-access)

nautilus::engine::NautilusEngine makeEngine(EngineMode mode)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", mode == EngineMode::Compiler);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    options.setOption("mlir.enableMultithreading", false);
    return {options};
}

Schema<QualifiedUnboundField, Ordered> createSchemaFromDataTypes(const std::vector<DataType>& dataTypes)
{
    return views::enumerate(dataTypes)
        | std::views::transform(
               [](const auto& pair)
               {
                   const auto& [typeIdx, dataType] = pair;
                   const auto name = Record::RecordFieldIdentifier(Identifier::parse("field" + std::to_string(typeIdx)));
                   return QualifiedUnboundField{name, dataType};
               })
        | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>();
}

rc::Gen<std::vector<DataType>> genDataTypeSchema(std::span<const DataType::Type> pool, size_t minFields, size_t maxFields)
{
    return rc::gen::exec(
        [pool = std::vector<DataType::Type>(pool.begin(), pool.end()), minFields, maxFields]()
        {
            const auto numFields = *rc::gen::inRange(minFields, maxFields + 1);
            std::vector<DataType> schema;
            schema.reserve(numFields);
            for (size_t i = 0; i < numFields; ++i)
            {
                const auto idx = *rc::gen::inRange(static_cast<size_t>(0), pool.size());
                const auto nullable = *rc::gen::arbitrary<bool>() ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
                schema.emplace_back(pool[idx], nullable);
            }
            return schema;
        });
}

rc::Gen<AnyVec> genAnyVec(std::vector<DataType> types)
{
    return rc::gen::exec(
        [types = std::move(types)]()
        {
            AnyVec result;
            result.reserve(types.size());
            for (const auto& dataType : types)
            {
                switch (dataType.type)
                {
                    case DataType::Type::UINT8:
                        result.push_back(genScalarAny<uint8_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT16:
                        result.push_back(genScalarAny<uint16_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT32:
                        result.push_back(genScalarAny<uint32_t>(dataType.nullable));
                        break;
                    case DataType::Type::UINT64:
                        result.push_back(genScalarAny<uint64_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT8:
                        result.push_back(genScalarAny<int8_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT16:
                        result.push_back(genScalarAny<int16_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT32:
                        result.push_back(genScalarAny<int32_t>(dataType.nullable));
                        break;
                    case DataType::Type::INT64:
                        result.push_back(genScalarAny<int64_t>(dataType.nullable));
                        break;
                    case DataType::Type::FLOAT32:
                        result.push_back(genScalarAny<float>(dataType.nullable));
                        break;
                    case DataType::Type::FLOAT64:
                        result.push_back(genScalarAny<double>(dataType.nullable));
                        break;
                    case DataType::Type::VARSIZED: {
                        if (dataType.nullable)
                        {
                            const bool isNull = *rc::gen::arbitrary<bool>();
                            if (isNull)
                            {
                                result.emplace_back(std::optional<std::string>{});
                                break;
                            }
                            auto str = *rc::gen::container<std::string>(rc::gen::inRange<char>(PRINTABLE_ASCII_MIN, PRINTABLE_ASCII_MAX));
                            if (str.size() > MAX_VARSIZED_LEN)
                            {
                                str.resize(MAX_VARSIZED_LEN);
                            }
                            result.emplace_back(std::optional<std::string>{std::move(str)});
                            break;
                        }
                        auto str = *rc::gen::container<std::string>(rc::gen::inRange<char>(PRINTABLE_ASCII_MIN, PRINTABLE_ASCII_MAX));
                        if (str.size() > MAX_VARSIZED_LEN)
                        {
                            str.resize(MAX_VARSIZED_LEN);
                        }
                        result.emplace_back(std::move(str));
                        break;
                    }
                    case DataType::Type::BOOLEAN:
                    case DataType::Type::CHAR:
                    case DataType::Type::UNDEFINED:
                        throw TestException("Unsupported type for genAnyVec");
                }
            }
            return result;
        });
}

int compareAnyField(const std::any& lhs, const std::any& rhs, DataType type)
{
    switch (type.type)
    {
        case DataType::Type::UINT8:
            return compareTyped<uint8_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT16:
            return compareTyped<uint16_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT32:
            return compareTyped<uint32_t>(lhs, rhs, type.nullable);
        case DataType::Type::UINT64:
            return compareTyped<uint64_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT8:
            return compareTyped<int8_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT16:
            return compareTyped<int16_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT32:
            return compareTyped<int32_t>(lhs, rhs, type.nullable);
        case DataType::Type::INT64:
            return compareTyped<int64_t>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT32:
            return compareTyped<float>(lhs, rhs, type.nullable);
        case DataType::Type::FLOAT64:
            return compareTyped<double>(lhs, rhs, type.nullable);
        case DataType::Type::VARSIZED:
            if (type.nullable)
            {
                const auto& left = std::any_cast<const std::optional<std::string>&>(lhs);
                const auto& right = std::any_cast<const std::optional<std::string>&>(rhs);
                if (left < right)
                {
                    return -1;
                }
                if (left > right)
                {
                    return 1;
                }
                return 0;
            }
            return std::any_cast<const std::string&>(lhs).compare(std::any_cast<const std::string&>(rhs));
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw TestException("Unsupported type for compareAnyField");
    }
    std::unreachable();
}

bool anyVecsEqual(const AnyVec& lhs, const AnyVec& rhs, const std::vector<DataType>& types)
{
    return std::ranges::all_of(
        std::views::zip(lhs, rhs, types),
        [](const auto& entry)
        {
            const auto& [left, right, dataType] = entry;
            return compareAnyField(left, right, dataType) == 0;
        });
}

void storeVarValToAnyVec(const nautilus::val<AnyVec*>& out, uint64_t pos, const VarVal& value, const DataType& dataType)
{
    switch (dataType.type)
    {
        case DataType::Type::UINT8:
            storeScalarToAnyVec<uint8_t>(out, pos, value.getRawValueAs<nautilus::val<uint8_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::UINT16:
            storeScalarToAnyVec<uint16_t>(out, pos, value.getRawValueAs<nautilus::val<uint16_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::UINT32:
            storeScalarToAnyVec<uint32_t>(out, pos, value.getRawValueAs<nautilus::val<uint32_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::UINT64:
            storeScalarToAnyVec<uint64_t>(out, pos, value.getRawValueAs<nautilus::val<uint64_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT8:
            storeScalarToAnyVec<int8_t>(out, pos, value.getRawValueAs<nautilus::val<int8_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT16:
            storeScalarToAnyVec<int16_t>(out, pos, value.getRawValueAs<nautilus::val<int16_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT32:
            storeScalarToAnyVec<int32_t>(out, pos, value.getRawValueAs<nautilus::val<int32_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::INT64:
            storeScalarToAnyVec<int64_t>(out, pos, value.getRawValueAs<nautilus::val<int64_t>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::FLOAT32:
            storeScalarToAnyVec<float>(out, pos, value.getRawValueAs<nautilus::val<float>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::FLOAT64:
            storeScalarToAnyVec<double>(out, pos, value.getRawValueAs<nautilus::val<double>>(), dataType.nullable, value.isNull());
            break;
        case DataType::Type::VARSIZED: {
            const auto vsd = value.getRawValueAs<VariableSizedData>();
            if (dataType.nullable)
            {
                nautilus::invoke(
                    /// NOLINTNEXTLINE(readability-non-const-parameter): ptr matches the val<int8_t*> signature emitted by VariableSizedData::getContent().
                    +[](AnyVec* anyVec, uint64_t pos, int8_t* ptr, uint64_t len, bool null)
                    {
                        if (null)
                        {
                            (*anyVec)[pos] = std::any{std::optional<std::string>{}};
                            return;
                        }
                        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                        const auto* const cptr = reinterpret_cast<const char*>(ptr);
                        (*anyVec)[pos] = std::any{std::optional<std::string>{std::string(cptr, len)}};
                    },
                    out,
                    nautilus::val<uint64_t>{pos},
                    vsd.getContent(),
                    vsd.getSize(),
                    value.isNull());
                break;
            }
            nautilus::invoke(
                /// NOLINTNEXTLINE(readability-non-const-parameter): ptr matches the val<int8_t*> signature emitted by VariableSizedData::getContent().
                +[](AnyVec* anyVec, uint64_t pos, int8_t* ptr, uint64_t len)
                {
                    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                    const auto* const cptr = reinterpret_cast<const char*>(ptr);
                    (*anyVec)[pos] = std::any{std::string(cptr, len)};
                },
                out,
                nautilus::val<uint64_t>{pos},
                vsd.getContent(),
                vsd.getSize());
            break;
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw TestException("Unsupported type for TestablePagedVector");
    }
}

nautilus::val<bool> checkIfNullInAnyVec(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType::Type type)
{
    return nautilus::invoke(
        +[](AnyVec* anyVec, uint64_t pos, DataType::Type fieldType) -> bool
        {
            const auto& entry = (*anyVec)[pos];
            switch (fieldType)
            {
                case DataType::Type::UINT8:
                    return !std::any_cast<std::optional<uint8_t>>(entry).has_value();
                case DataType::Type::UINT16:
                    return !std::any_cast<std::optional<uint16_t>>(entry).has_value();
                case DataType::Type::UINT32:
                    return !std::any_cast<std::optional<uint32_t>>(entry).has_value();
                case DataType::Type::UINT64:
                    return !std::any_cast<std::optional<uint64_t>>(entry).has_value();
                case DataType::Type::INT8:
                    return !std::any_cast<std::optional<int8_t>>(entry).has_value();
                case DataType::Type::INT16:
                    return !std::any_cast<std::optional<int16_t>>(entry).has_value();
                case DataType::Type::INT32:
                    return !std::any_cast<std::optional<int32_t>>(entry).has_value();
                case DataType::Type::INT64:
                    return !std::any_cast<std::optional<int64_t>>(entry).has_value();
                case DataType::Type::FLOAT32:
                    return !std::any_cast<std::optional<float>>(entry).has_value();
                case DataType::Type::FLOAT64:
                    return !std::any_cast<std::optional<double>>(entry).has_value();
                case DataType::Type::VARSIZED:
                    return !std::any_cast<std::optional<std::string>>(entry).has_value();
                default:
                    std::unreachable();
            }
        },
        rec,
        nautilus::val<uint64_t>{fieldIdx},
        nautilus::val<DataType::Type>{type});
}

nautilus::val<AnyVec*> anyVecPushBack(const nautilus::val<std::vector<AnyVec>*>& vec, const nautilus::val<size_t>& numberOfFields)
{
    return nautilus::invoke(
        +[](std::vector<AnyVec>* outer, size_t numberOfFields)
        {
            outer->emplace_back(numberOfFields);
            return &outer->back();
        },
        vec,
        numberOfFields);
}

VarVal buildVarVal(const nautilus::val<AnyVec*>& rec, uint64_t fieldIdx, DataType dataType)
{
    const nautilus::val<bool> isNull = dataType.nullable ? checkIfNullInAnyVec(rec, fieldIdx, dataType.type) : nautilus::val<bool>{false};
    switch (dataType.type)
    {
        case DataType::Type::UINT8:
            return {fetchScalarFromAnyVec<uint8_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::UINT16:
            return {fetchScalarFromAnyVec<uint16_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::UINT32:
            return {fetchScalarFromAnyVec<uint32_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::UINT64:
            return {fetchScalarFromAnyVec<uint64_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT8:
            return {fetchScalarFromAnyVec<int8_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT16:
            return {fetchScalarFromAnyVec<int16_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT32:
            return {fetchScalarFromAnyVec<int32_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::INT64:
            return {fetchScalarFromAnyVec<int64_t>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::FLOAT32:
            return {fetchScalarFromAnyVec<float>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::FLOAT64:
            return {fetchScalarFromAnyVec<double>(rec, fieldIdx, dataType.nullable), dataType.nullable, isNull};
        case DataType::Type::VARSIZED: {
            auto ptr = nautilus::invoke(
                +[](AnyVec* anyVec, uint64_t pos, bool nullable) -> int8_t*
                {
                    const char* data = nullptr;
                    if (nullable)
                    {
                        const auto& opt = std::any_cast<const std::optional<std::string>&>((*anyVec)[pos]);
                        data = opt.has_value() ? opt->data() : nullptr;
                    }
                    else
                    {
                        data = std::any_cast<const std::string&>((*anyVec)[pos]).data();
                    }
                    /// reinterpret_cast: punning char* to int8_t* (same representation).
                    /// const_cast: the VariableSizedData/Nautilus API requires a mutable int8_t* even though
                    /// this code path only reads from the buffer; the reference behind `data` originates from
                    /// the std::any/std::string store and is not actually written through.
                    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast,cppcoreguidelines-pro-type-reinterpret-cast)
                    return const_cast<int8_t*>(reinterpret_cast<const int8_t*>(data));
                },
                rec,
                nautilus::val<uint64_t>{fieldIdx},
                nautilus::val<bool>{dataType.nullable});
            auto len = nautilus::invoke(
                +[](AnyVec* anyVec, uint64_t pos, bool nullable) -> uint64_t
                {
                    if (nullable)
                    {
                        const auto& opt = std::any_cast<const std::optional<std::string>&>((*anyVec)[pos]);
                        return opt.has_value() ? opt->size() : 0;
                    }
                    return std::any_cast<const std::string&>((*anyVec)[pos]).size();
                },
                rec,
                nautilus::val<uint64_t>{fieldIdx},
                nautilus::val<bool>{dataType.nullable});
            return {VariableSizedData(ptr, len), dataType.nullable, isNull};
        }
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            break;
    }
    throw TestException("Unsupported type for TestablePagedVector");
}

/// NOLINTEND(misc-include-cleaner, bugprone-unchecked-optional-access)

}
