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

#include <cstdint>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <type_traits>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <fmt/format.h>
#include <google/protobuf/json/json.h>
#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SerializableVariantDescriptor.pb.h>

namespace NES::Configurations
{

Descriptor::Descriptor(DescriptorConfig::Config&& config) : config(std::move(config))
{
}

SerializableVariantDescriptor descriptorConfigTypeToProto(const NES::Configurations::DescriptorConfig::ConfigType& var)
{
    SerializableVariantDescriptor protoVar;
    std::visit(
        [&protoVar]<typename T>(T&& arg)
        {
            /// Remove const, volatile, and reference to simplify type matching
            using U = std::remove_cvref_t<T>;
            if constexpr (std::is_same_v<U, int32_t>)
            {
                protoVar.set_int_value(arg);
            }
            else if constexpr (std::is_same_v<U, uint32_t>)
            {
                protoVar.set_uint_value(arg);
            }
            else if constexpr (std::is_same_v<U, int64_t>)
            {
                protoVar.set_long_value(arg);
            }
            else if constexpr (std::is_same_v<U, uint64_t>)
            {
                protoVar.set_ulong_value(arg);
            }
            else if constexpr (std::is_same_v<U, bool>)
            {
                protoVar.set_bool_value(arg);
            }
            else if constexpr (std::is_same_v<U, char>)
            {
                protoVar.set_char_value(arg);
            }
            else if constexpr (std::is_same_v<U, float>)
            {
                protoVar.set_float_value(arg);
            }
            else if constexpr (std::is_same_v<U, double>)
            {
                protoVar.set_double_value(arg);
            }
            else if constexpr (std::is_same_v<U, std::string>)
            {
                protoVar.set_string_value(arg);
            }
            else if constexpr (std::is_same_v<U, Configurations::EnumWrapper>)
            {
                protoVar.mutable_enum_value()->set_value(arg.getValue());
            }
            else if constexpr (std::is_same_v<U, NES::FunctionList>)
            {
                protoVar.mutable_function_list()->CopyFrom(arg);
            }
            else if constexpr (std::is_same_v<U, NES::AggregationFunctionList>)
            {
                protoVar.mutable_aggregation_function_list()->CopyFrom(arg);
            }
            else if constexpr (std::is_same_v<U, NES::WindowInfos>)
            {
                protoVar.mutable_window_infos()->CopyFrom(arg);
            }
            else
            {
                static_assert(!std::is_same_v<U, U>, "Unsupported type in SourceDescriptorConfigTypeToProto"); /// is_same_v for logging T
            }
        },
        var);
    return protoVar;
}

Configurations::DescriptorConfig::ConfigType protoToDescriptorConfigType(const SerializableVariantDescriptor& protoVar)
{
    switch (protoVar.value_case())
    {
        case SerializableVariantDescriptor::kIntValue:
            return protoVar.int_value();
        case SerializableVariantDescriptor::kUintValue:
            return protoVar.uint_value();
        case SerializableVariantDescriptor::kLongValue:
            return protoVar.long_value();
        case SerializableVariantDescriptor::kUlongValue:
            return protoVar.ulong_value();
        case SerializableVariantDescriptor::kBoolValue:
            return protoVar.bool_value();
        case SerializableVariantDescriptor::kCharValue:
            return static_cast<char>(protoVar.char_value()); /// Convert (fixed32) ascii number to char.
        case SerializableVariantDescriptor::kFloatValue:
            return protoVar.float_value();
        case SerializableVariantDescriptor::kDoubleValue:
            return protoVar.double_value();
        case SerializableVariantDescriptor::kStringValue:
            return protoVar.string_value();
        case SerializableVariantDescriptor::kEnumValue:
            return Configurations::EnumWrapper(protoVar.enum_value().value());
        case SerializableVariantDescriptor::kFunctionList:
            return protoVar.function_list();
        case SerializableVariantDescriptor::kAggregationFunctionList:
            return protoVar.aggregation_function_list();
        case SerializableVariantDescriptor::kWindowInfos:
            return protoVar.window_infos();
        default:
            std::string protoVarAsJson;
            /// Log proto variable as json, in exception, if possible.
            if (const auto conversionResult = google::protobuf::json::MessageToJsonString(protoVar, &protoVarAsJson); conversionResult.ok())
            {
                throw CannotSerialize(fmt::format("Unknown variant type: {}", protoVarAsJson));
            }
            throw CannotSerialize("Unknown variant type.");
    }
}

/// Define a ConfigPrinter to generate print functions for all options of the std::variant 'ConfigType'.
struct ConfigPrinter
{
    std::ostream& out;

    template <typename T>
    void operator()(const T& value) const
    {
        if constexpr (!std::is_enum_v<T>)
        {
            out << value;
        }
        else
        {
            out << std::string(magic_enum::enum_name(value));
        }
    }
};
std::ostream& operator<<(std::ostream& out, const DescriptorConfig::Config& config)
{
    if (config.empty())
    {
        return out;
    }
    out << config.begin()->first << ": ";
    std::visit(ConfigPrinter{out}, config.begin()->second);
    for (const auto& [key, value] : config | std::views::drop(1))
    {
        out << ", " << key << ": ";
        std::visit(ConfigPrinter{out}, value);
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, const Descriptor& descriptor)
{
    return out << "\nDescriptor( " << descriptor.config << " )";
}

std::string Descriptor::toStringConfig() const
{
    std::stringstream ss;
    ss << this->config;
    return ss.str();
}

}
