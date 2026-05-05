// WASM-specific Descriptor.cpp: omits the protobuf serialization helpers so the
// validator does not need protobuf runtime headers in the browser. Only the
// non-proto methods (constructor, ostream operators, toStringConfig, and the
// reflection-based getReflectedConfig/unreflectConfig) are needed by the
// SourceDescriptor/SinkDescriptor code paths the WASM build pulls in. Kept in
// sync with nes-configurations/src/Configurations/Descriptor.cpp on main.
#include <Configurations/Descriptor.hpp>

#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>

#include <Configurations/Enums/EnumWrapper.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

Descriptor::Descriptor(DescriptorConfig::Config&& config) : config(std::move(config))
{
}

namespace
{
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
}

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

namespace
{
struct ReflectedDescriptorConfig
{
    std::unordered_map<std::string, std::pair<std::string, Reflected>> config;
};
}

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
                // wasm32: ConfigType variant carries `unsigned long` as a distinct alternative
                // (size_t lowers to it). Reflected as uint32 since `unsigned long` is 32-bit on wasm32.
                [&](const unsigned long val) { reflectedConfig.config[key] = std::make_pair("uint32_t", reflect(static_cast<uint32_t>(val))); },
                [&](const bool val) { reflectedConfig.config[key] = std::make_pair("bool", reflect(val)); },
                [&](const char val) { reflectedConfig.config[key] = std::make_pair("char", reflect(val)); },
                [&](const float val) { reflectedConfig.config[key] = std::make_pair("float", reflect(val)); },
                [&](const double val) { reflectedConfig.config[key] = std::make_pair("double", reflect(val)); },
                [&](const std::string& val) { reflectedConfig.config[key] = std::make_pair("string", reflect(val)); },
                [&](const EnumWrapper& val) { reflectedConfig.config[key] = std::make_pair("EnumWrapper", reflect(val)); }},
            value);
    }
    return reflect(reflectedConfig);
}

DescriptorConfig::Config Descriptor::unreflectConfig(const Reflected& rfl)
{
    auto reflectedConfig = unreflect<ReflectedDescriptorConfig>(rfl);
    DescriptorConfig::Config config;
    for (const auto& [key, pair] : reflectedConfig.config)
    {
        const auto& [type, value] = pair;
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
        else
        {
            throw CannotDeserialize("Unknown descriptor config type {}", type);
        }
    }
    return config;
}

}
