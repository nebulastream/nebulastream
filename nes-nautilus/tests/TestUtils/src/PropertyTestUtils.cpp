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
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
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
        case DataType::Type::VARSIZED:
        {
            const auto& vsd = val.cast<VariableSizedData>();
            const auto* ptr = nautilus::details::RawValueResolver<int8_t*>::getRawValue(vsd.getContent());
            const auto len = nautilus::details::RawValueResolver<uint64_t>::getRawValue(vsd.getSize());
            return std::string(reinterpret_cast<const char*>(ptr), len);
        }
        default:
            throw std::runtime_error("Unsupported type for varValToAny");
    }
}

std::vector<std::any> recordToAnyVec(
    const Record& record,
    const std::vector<Record::RecordFieldIdentifier>& projection,
    const std::vector<DataType::Type>& types)
{
    std::vector<std::any> result;
    result.reserve(projection.size());
    for (size_t i = 0; i < projection.size(); ++i)
    {
        result.push_back(varValToAny(record.read(projection[i]), types[i]));
    }
    return result;
}

int compareAnyField(const std::any& a, const std::any& b, DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::UINT8: { auto l = std::any_cast<uint8_t>(a), r = std::any_cast<uint8_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::UINT16: { auto l = std::any_cast<uint16_t>(a), r = std::any_cast<uint16_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::UINT32: { auto l = std::any_cast<uint32_t>(a), r = std::any_cast<uint32_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::UINT64: { auto l = std::any_cast<uint64_t>(a), r = std::any_cast<uint64_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::INT8: { auto l = std::any_cast<int8_t>(a), r = std::any_cast<int8_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::INT16: { auto l = std::any_cast<int16_t>(a), r = std::any_cast<int16_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::INT32: { auto l = std::any_cast<int32_t>(a), r = std::any_cast<int32_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::INT64: { auto l = std::any_cast<int64_t>(a), r = std::any_cast<int64_t>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::FLOAT32: { auto l = std::any_cast<float>(a), r = std::any_cast<float>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::FLOAT64: { auto l = std::any_cast<double>(a), r = std::any_cast<double>(b); return (l < r) ? -1 : (l > r) ? 1 : 0; }
        case DataType::Type::VARSIZED: { const auto& l = std::any_cast<const std::string&>(a); const auto& r = std::any_cast<const std::string&>(b); return l.compare(r); }
        default: throw std::runtime_error("Unsupported type for compareAnyField");
    }
}

bool AnyVecLess::operator()(const std::vector<std::any>& a, const std::vector<std::any>& b) const
{
    for (size_t i = 0; i < types.size(); ++i)
    {
        const int cmp = compareAnyField(a[i], b[i], types[i]);
        if (cmp != 0)
            return cmp < 0;
    }
    return false;
}

bool anyVecsEqual(const std::vector<std::any>& a, const std::vector<std::any>& b, const std::vector<DataType::Type>& types)
{
    for (size_t i = 0; i < types.size(); ++i)
    {
        if (compareAnyField(a[i], b[i], types[i]) != 0)
            return false;
    }
    return true;
}

}
