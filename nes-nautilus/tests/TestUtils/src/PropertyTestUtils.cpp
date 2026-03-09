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

#include <PropertyTestUtils.hpp>

#include <stdexcept>
#include <string>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <NautilusTestUtils.hpp>
#include <val_details.hpp>

namespace NES::TestUtils
{

const std::vector<DataType::Type> ALL_KEY_TYPES = {
    DataType::Type::UINT8,
    DataType::Type::UINT16,
    DataType::Type::UINT32,
    DataType::Type::UINT64,
    DataType::Type::INT8,
    DataType::Type::INT16,
    DataType::Type::INT32,
    DataType::Type::INT64,
    DataType::Type::VARSIZED,
};

const std::vector<DataType::Type> ALL_VALUE_TYPES = {
    DataType::Type::UINT8,
    DataType::Type::UINT16,
    DataType::Type::UINT32,
    DataType::Type::UINT64,
    DataType::Type::INT8,
    DataType::Type::INT16,
    DataType::Type::INT32,
    DataType::Type::INT64,
    DataType::Type::FLOAT32,
    DataType::Type::FLOAT64,
    DataType::Type::VARSIZED,
};

rc::Gen<std::vector<DataType::Type>> genTypeSchema(std::vector<DataType::Type> pool, size_t minFields, size_t maxFields)
{
    return rc::gen::exec(
        [pool = std::move(pool), minFields, maxFields]()
        {
            const auto numFields = *rc::gen::inRange(minFields, maxFields + 1);
            std::vector<DataType::Type> schema;
            schema.reserve(numFields);
            for (size_t i = 0; i < numFields; ++i)
            {
                const auto idx = *rc::gen::inRange(static_cast<size_t>(0), pool.size());
                schema.push_back(pool[idx]);
            }
            return schema;
        });
}

uint64_t estimateSchemaSize(const std::vector<DataType::Type>& types)
{
    auto schema = NautilusTestUtils::createSchemaFromBasicTypes(types);
    return schema.getSizeOfSchemaInBytes();
}

std::any varValToAny(const VarVal& val, DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::UINT8:
            return nautilus::details::RawValueResolver<uint8_t>::getRawValue(val.cast<nautilus::val<uint8_t>>());
        case DataType::Type::UINT16:
            return nautilus::details::RawValueResolver<uint16_t>::getRawValue(val.cast<nautilus::val<uint16_t>>());
        case DataType::Type::UINT32:
            return nautilus::details::RawValueResolver<uint32_t>::getRawValue(val.cast<nautilus::val<uint32_t>>());
        case DataType::Type::UINT64:
            return nautilus::details::RawValueResolver<uint64_t>::getRawValue(val.cast<nautilus::val<uint64_t>>());
        case DataType::Type::INT8:
            return nautilus::details::RawValueResolver<int8_t>::getRawValue(val.cast<nautilus::val<int8_t>>());
        case DataType::Type::INT16:
            return nautilus::details::RawValueResolver<int16_t>::getRawValue(val.cast<nautilus::val<int16_t>>());
        case DataType::Type::INT32:
            return nautilus::details::RawValueResolver<int32_t>::getRawValue(val.cast<nautilus::val<int32_t>>());
        case DataType::Type::INT64:
            return nautilus::details::RawValueResolver<int64_t>::getRawValue(val.cast<nautilus::val<int64_t>>());
        case DataType::Type::FLOAT32:
            return nautilus::details::RawValueResolver<float>::getRawValue(val.cast<nautilus::val<float>>());
        case DataType::Type::FLOAT64:
            return nautilus::details::RawValueResolver<double>::getRawValue(val.cast<nautilus::val<double>>());
        case DataType::Type::VARSIZED: {
            const auto& vsd = val.cast<VariableSizedData>();
            const auto* ptr = nautilus::details::RawValueResolver<int8_t*>::getRawValue(vsd.getContent());
            const auto len = nautilus::details::RawValueResolver<uint64_t>::getRawValue(vsd.getSize());
            return std::string(reinterpret_cast<const char*>(ptr), len);
        }
        default:
            throw std::runtime_error("Unsupported type for varValToAny");
    }
}

std::vector<std::any>
recordToAnyVec(const Record& record, const std::vector<Record::RecordFieldIdentifier>& projection, const std::vector<DataType::Type>& types)
{
    std::vector<std::any> result;
    result.reserve(projection.size());
    for (size_t i = 0; i < projection.size(); ++i)
    {
        result.push_back(varValToAny(record.read(projection[i]), types[i]));
    }
    return result;
}

namespace
{
template <typename T>
int compareTyped(const std::any& lhs, const std::any& rhs)
{
    const auto left = std::any_cast<T>(lhs);
    const auto right = std::any_cast<T>(rhs);
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
} /// anonymous namespace

int compareAnyField(const std::any& lhs, const std::any& rhs, DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::UINT8:
            return compareTyped<uint8_t>(lhs, rhs);
        case DataType::Type::UINT16:
            return compareTyped<uint16_t>(lhs, rhs);
        case DataType::Type::UINT32:
            return compareTyped<uint32_t>(lhs, rhs);
        case DataType::Type::UINT64:
            return compareTyped<uint64_t>(lhs, rhs);
        case DataType::Type::INT8:
            return compareTyped<int8_t>(lhs, rhs);
        case DataType::Type::INT16:
            return compareTyped<int16_t>(lhs, rhs);
        case DataType::Type::INT32:
            return compareTyped<int32_t>(lhs, rhs);
        case DataType::Type::INT64:
            return compareTyped<int64_t>(lhs, rhs);
        case DataType::Type::FLOAT32:
            return compareTyped<float>(lhs, rhs);
        case DataType::Type::FLOAT64:
            return compareTyped<double>(lhs, rhs);
        case DataType::Type::VARSIZED: {
            const auto& left = std::any_cast<const std::string&>(lhs);
            const auto& right = std::any_cast<const std::string&>(rhs);
            return left.compare(right);
        }
        default:
            throw std::runtime_error("Unsupported type for compareAnyField");
    }
}

bool AnyVecLess::operator()(const std::vector<std::any>& lhs, const std::vector<std::any>& rhs) const
{
    for (size_t i = 0; i < types.size(); ++i)
    {
        const int cmp = compareAnyField(lhs[i], rhs[i], types[i]);
        if (cmp != 0)
        {
            return cmp < 0;
        }
    }
    return false;
}

bool anyVecsEqual(const std::vector<std::any>& lhs, const std::vector<std::any>& rhs, const std::vector<DataType::Type>& types)
{
    for (size_t i = 0; i < types.size(); ++i)
    {
        if (compareAnyField(lhs[i], rhs[i], types[i]) != 0)
        {
            return false;
        }
    }
    return true;
}

}
