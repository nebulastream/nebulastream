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

#include "SubstraitTypes.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <substrait/extensions/extensions.pb.h>
#include <substrait/plan.pb.h>
#include <substrait/type.pb.h>
#include <ErrorHandling.hpp>

namespace NES::Calcite
{
namespace detail
{

/// Substrait has no unsigned integers; Calcite's UTINYINT..UBIGINT arrive as the user-defined types u8..u64 of this
/// extension (see Unsigned.java).
UserDefinedTypes unsignedTypes(const substrait::Plan& plan)
{
    static constexpr std::string_view UNSIGNED_URN = "extension:io.substrait:unsigned_integers";
    static const std::unordered_map<std::string, DataType::Type> names{
        {"u8", DataType::Type::UINT8}, {"u16", DataType::Type::UINT16}, {"u32", DataType::Type::UINT32}, {"u64", DataType::Type::UINT64}};
    std::unordered_set<uint32_t> unsignedUrns;
    for (const auto& urn : plan.extension_urns())
    {
        if (urn.urn() == UNSIGNED_URN)
        {
            unsignedUrns.insert(urn.extension_urn_anchor());
        }
    }
    UserDefinedTypes types;
    for (const auto& extension : plan.extensions())
    {
        if (extension.has_extension_type() and unsignedUrns.contains(extension.extension_type().extension_urn_reference()))
        {
            if (const auto found = names.find(extension.extension_type().name()); found != names.end())
            {
                types.emplace(extension.extension_type().type_anchor(), found->second);
            }
        }
    }
    return types;
}

DataType toDataType(const substrait::Type& type, const UserDefinedTypes& userDefinedTypes)
{
    const auto make = [](const DataType::Type nesType, const substrait::Type::Nullability nullability)
    {
        return DataTypeProvider::provideDataType(
            nesType,
            nullability == substrait::Type::NULLABILITY_NULLABLE ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    };
    switch (type.kind_case())
    {
        case substrait::Type::kBool:
            return make(DataType::Type::BOOLEAN, type.bool_().nullability());
        case substrait::Type::kI8:
            return make(DataType::Type::INT8, type.i8().nullability());
        case substrait::Type::kI16:
            return make(DataType::Type::INT16, type.i16().nullability());
        case substrait::Type::kI32:
            return make(DataType::Type::INT32, type.i32().nullability());
        case substrait::Type::kI64:
            return make(DataType::Type::INT64, type.i64().nullability());
        case substrait::Type::kFp32:
            return make(DataType::Type::FLOAT32, type.fp32().nullability());
        case substrait::Type::kFp64:
            return make(DataType::Type::FLOAT64, type.fp64().nullability());
        case substrait::Type::kString:
            return make(DataType::Type::VARSIZED, type.string().nullability());
        case substrait::Type::kVarchar:
            return make(DataType::Type::VARSIZED, type.varchar().nullability());
        case substrait::Type::kFixedChar:
            return make(DataType::Type::CHAR, type.fixed_char().nullability());
        /// Bytes are variable-sized data to us, as text is (e.g. FROM_BASE64's result).
        case substrait::Type::kBinary:
            return make(DataType::Type::VARSIZED, type.binary().nullability());
        case substrait::Type::kFixedBinary:
            return make(DataType::Type::VARSIZED, type.fixed_binary().nullability());
        /// A timestamp is unix milliseconds (UTC) to us, with or without a time zone (e.g. TIMESTAMP(text) has one).
        case substrait::Type::kPrecisionTimestamp:
            return make(DataType::Type::UINT64, type.precision_timestamp().nullability());
        case substrait::Type::kPrecisionTimestampTz:
            return make(DataType::Type::UINT64, type.precision_timestamp_tz().nullability());
        case substrait::Type::kUserDefined:
            if (const auto found = userDefinedTypes.find(type.user_defined().type_reference()); found != userDefinedTypes.end())
            {
                return make(found->second, type.user_defined().nullability());
            }
            throw UnsupportedQuery("Substrait type {} is not supported", type.ShortDebugString());
        default:
            throw UnsupportedQuery("Substrait type {} is not supported", type.ShortDebugString());
    }
}

std::unordered_map<uint32_t, std::string> functionNames(const substrait::Plan& plan)
{
    std::unordered_map<uint32_t, std::string> functions;
    for (const auto& extension : plan.extensions())
    {
        if (extension.has_extension_function())
        {
            /// Names carry their signature (gt:any_any); only the function matters to us.
            const auto& name = extension.extension_function().name();
            functions.emplace(extension.extension_function().function_anchor(), name.substr(0, name.find(':')));
        }
    }
    return functions;
}
}

}
