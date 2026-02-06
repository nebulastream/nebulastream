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
#include <Configurations/Descriptor.hpp>

#include <cstdint>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>

#include <Configurations/ConfigSerialization.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <google/protobuf/json/json.h>
#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

Descriptor::Descriptor(DescriptorConfig::Config&& config) : config(std::move(config))
{
}

SerializableVariantDescriptor descriptorConfigTypeToProto(const DescriptorConfig::ConfigType& var)
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
            else if constexpr (std::is_same_v<U, EnumWrapper>)
            {
                protoVar.mutable_enum_value()->set_value(arg.getValue());
            }
            else if constexpr (std::is_same_v<U, FunctionList>)
            {
                protoVar.mutable_function_list()->CopyFrom(arg);
            }
            else if constexpr (std::is_same_v<U, ProjectionList>)
            {
                protoVar.mutable_projections()->CopyFrom(arg);
            }
            else if constexpr (std::is_same_v<U, AggregationFunctionList>)
            {
                protoVar.mutable_aggregation_function_list()->CopyFrom(arg);
            }
            else if constexpr (std::is_same_v<U, WindowInfos>)
            {
                protoVar.mutable_window_infos()->CopyFrom(arg);
            }
            else if constexpr (std::is_same_v<U, UInt64List>)
            {
                protoVar.mutable_ulongs()->CopyFrom(arg);
            }
            else
            {
                static_assert(!std::is_same_v<U, U>, "Unsupported type in SourceDescriptorConfigTypeToProto"); /// is_same_v for logging T
            }
        },
        var);
    return protoVar;
}

DescriptorConfig::ConfigType protoToDescriptorConfigType(const SerializableVariantDescriptor& protoVar)
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
            return EnumWrapper(protoVar.enum_value().value());
        case SerializableVariantDescriptor::kFunctionList:
            return protoVar.function_list();
        case SerializableVariantDescriptor::kAggregationFunctionList:
            return protoVar.aggregation_function_list();
        case SerializableVariantDescriptor::kProjections:
            return protoVar.projections();
        case SerializableVariantDescriptor::kWindowInfos:
            return protoVar.window_infos();
        case SerializableVariantDescriptor::kUlongs:
            return protoVar.ulongs();
        case NES::SerializableVariantDescriptor::VALUE_NOT_SET:
            throw CannotSerialize("Protobuf oneOf has no value");
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

struct ReflectedDescriptorConfig
{
    std::unordered_map<std::string, std::pair<std::string, Reflected>> config;
};

Reflected Descriptor::getReflectedConfig() const
{
    ReflectedDescriptorConfig reflectedConfig;

    for (const auto& [key, value] : this->config)
    {
        std::visit(
            Overloaded{
                [&](const int32_t val) { reflectedConfig.config[key] = std::make_pair("int32_t", reflect(val)); },
                [&](const uint32_t val) { reflectedConfig.config[key] = std::make_pair("uint32_t", reflect(val)); },
                [&](const int64_t val) { reflectedConfig.config[key] = std::make_pair("int64_t", reflect(val)); },
                [&](const uint64_t val) { reflectedConfig.config[key] = std::make_pair("uint64_t", reflect(val)); },
                [&](bool val) { reflectedConfig.config[key] = std::make_pair("bool", reflect(val)); },
                [&](char val) { reflectedConfig.config[key] = std::make_pair("char", reflect(val)); },
                [&](float val) { reflectedConfig.config[key] = std::make_pair("float", reflect(val)); },
                [&](double val) { reflectedConfig.config[key] = std::make_pair("double", reflect(val)); },
                [&](const std::string& val) { reflectedConfig.config[key] = std::make_pair("string", reflect(val)); },
                [&](const EnumWrapper& val) { reflectedConfig.config[key] = std::make_pair("EnumWrapper", reflect(val)); },
                [&](const FunctionList& val) { reflectedConfig.config[key] = std::make_pair("FunctionList", reflect(val)); },
                [&](const AggregationFunctionList& val)
                { reflectedConfig.config[key] = std::make_pair("AggregationFunctionList", reflect(val)); },
                [&](const WindowInfos& val) { reflectedConfig.config[key] = std::make_pair("WindowInfos", reflect(val)); },
                [&](const ProjectionList& val) { reflectedConfig.config[key] = std::make_pair("ProjectionList", reflect(val)); },
                [&](const UInt64List& val) { reflectedConfig.config[key] = std::make_pair("UInt64List", reflect(val)); }},
            value);
    }

    return reflect(reflectedConfig);
}

DescriptorConfig::Config Descriptor::unreflectConfig(const Reflected& rfl)
{
    auto reflectedConfig = unreflect<ReflectedDescriptorConfig>(rfl);
    DescriptorConfig::Config config;

    for (auto [key, pair] : reflectedConfig.config)
    {
        auto [type, value] = pair;

        if (type == "int32_t")
        {
            config[key] = unreflect<int32_t>(value);
        }
        else if (type == "uint32_t")
        {
            config[key] = unreflect<uint32_t>(value);
        }
        else if (type == "int64_t")
        {
            config[key] = unreflect<int64_t>(value);
        }
        else if (type == "uint64_t")
        {
            config[key] = unreflect<uint64_t>(value);
        }
        else if (type == "bool")
        {
            config[key] = unreflect<bool>(value);
        }
        else if (type == "char")
        {
            config[key] = unreflect<char>(value);
        }
        else if (type == "float")
        {
            config[key] = unreflect<float>(value);
        }
        else if (type == "double")
        {
            config[key] = unreflect<double>(value);
        }
        else if (type == "string")
        {
            config[key] = unreflect<std::string>(value);
        }
        else if (type == "EnumWrapper")
        {
            config[key] = unreflect<EnumWrapper>(value);
        }
        else if (type == "FunctionList")
        {
            config[key] = unreflect<FunctionList>(value);
        }
        else if (type == "AggregationFunctionList")
        {
            config[key] = unreflect<AggregationFunctionList>(value);
        }
        else if (type == "WindowInfos")
        {
            config[key] = unreflect<WindowInfos>(value);
        }
        else if (type == "ProjectionList")
        {
            config[key] = unreflect<ProjectionList>(value);
        }
        else if (type == "UInt64List")
        {
            config[key] = unreflect<UInt64List>(value);
        }
        else
        {
            throw CannotDeserialize();
        }
    }


    return config;
}

}
