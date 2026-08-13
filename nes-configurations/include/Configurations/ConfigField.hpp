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
#include <any>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <limits>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Variant.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <nameof.hpp>

namespace NES
{
/// Integer literals are always signed: frontends never produce an unsigned literal, and a field
/// that needs an unsigned type lowers the int64_t with a range check (see downcastConfigValue).
using ConfigLiteral = std::variant<std::string, int64_t, double, bool, std::monostate, Schema<UnqualifiedUnboundField, Ordered>>;
template <typename T>
concept isConfigLiteral = requires(T t) { ConfigLiteral{std::in_place_type<T>, std::move(t)}; };

template <typename T>
std::function<Exception()> expectedType()
{
    return [] { return InvalidConfigParameter("Expected type: {}", NAMEOF_TYPE(T)); };
}

/// Frontends type integer-looking input as int64_t (see ConfigLiteral), so fields with a floating
/// point type accept both fractional and integer literals; the integer is widened to double.
inline std::expected<double, Exception> tryGetDoubleOrInt(const ConfigLiteral& literal, const std::function<Exception()>& orElse)
{
    if (const auto* asDouble = std::get_if<double>(&literal))
    {
        return *asDouble;
    }
    if (const auto* asInteger = std::get_if<int64_t>(&literal))
    {
        return static_cast<double>(*asInteger);
    }
    return std::unexpected{orElse()};
}

/// A single (possibly qualified) config assignment as a frontend produced it, e.g.
/// `'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES`
class LiteralConfigValue
{
    QualifiedIdentifier name;
    ConfigLiteral value;

public:
    LiteralConfigValue(QualifiedIdentifier name, ConfigLiteral value) : name(std::move(name)), value(std::move(value)) { }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    [[nodiscard]] ConfigLiteral getValue() const { return value; }

    friend bool operator==(const LiteralConfigValue& lhs, const LiteralConfigValue& rhs) = default;

    friend std::ostream& operator<<(std::ostream& os, const LiteralConfigValue& value) { return os << value.name; }
};

/// Declares a single typed config parameter: its name, how to instantiate it from a literal the
/// frontend passed, and (optionally) its default. The field type T is arbitrary — it does not
/// need to be serializable, because serialization of catalog objects goes through the
/// source-defined config struct, not through individual config fields.
template <typename T>
class ConfigField
{
    Identifier name;
    std::string description;
    std::type_index type;
    std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory;
    std::optional<std::function<T()>> defaultSupplier;
    std::optional<std::string> defaultDescription;

public:
    ConfigField(Identifier name, std::string description, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory)
        : name(std::move(name))
        , description(std::move(description))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier(std::nullopt)
        , defaultDescription(std::nullopt)
    {
    }

    ConfigField(
        Identifier name,
        std::string description,
        std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory,
        std::function<T()> defaultSupplier,
        std::string defaultDescription)
        : name(std::move(name))
        , description(std::move(description))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier(std::move(defaultSupplier))
        , defaultDescription(std::move(defaultDescription))
    {
    }

    ConfigField(
        Identifier name, std::string description, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory, T defaultValue)
    requires(fmt::formattable<T>)
        : name(std::move(name))
        , description(std::move(description))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier([defaultValue] { return defaultValue; })
        , defaultDescription(fmt::format("{}", defaultValue))
    {
    }

    ConfigField(
        Identifier name,
        std::string description,
        std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory,
        T defaultValue,
        std::string defaultDescription)
        : name(std::move(name))
        , description(std::move(description))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier([defaultValue] { return defaultValue; })
        , defaultDescription(std::move(defaultDescription))
    {
    }

    ConfigField(Identifier name, std::string description)
    requires isConfigLiteral<T>
        : name(std::move(name))
        , description(std::move(description))
        , type(std::type_index(typeid(T)))
        , factory([](const ConfigLiteral& literal) { return tryGetOr<T>(literal, expectedType<T>()); })
        , defaultSupplier(std::nullopt)
        , defaultDescription(std::nullopt)
    {
    }

    ConfigField(Identifier name, std::string description, T defaultValue)
    requires isConfigLiteral<T>
        : name(std::move(name))
        , description(std::move(description))
        , type(std::type_index(typeid(T)))
        , factory([](const ConfigLiteral& literal) { return tryGetOr<T>(literal, expectedType<T>()); })
        , defaultSupplier([defaultValue] { return defaultValue; })
        , defaultDescription(fmt::format("{}", defaultValue))
    {
    }

    ConfigField(const ConfigField& other) = delete;
    ConfigField(ConfigField&& other) noexcept = delete;
    ConfigField& operator=(const ConfigField& other) = delete;
    ConfigField& operator=(ConfigField&& other) noexcept = delete;

    [[nodiscard]] Identifier getName() const { return name; }

    [[nodiscard]] std::type_index getType() const { return type; }

    [[nodiscard]] std::function<std::expected<T, Exception>(const ConfigLiteral&)> getFactory() const { return factory; }

    [[nodiscard]] std::optional<std::function<T()>> getDefaultSupplier() const { return defaultSupplier; }

    [[nodiscard]] std::string_view getDescription() const { return description; }

    [[nodiscard]] std::optional<std::string_view> getDefaultDescription() const { return defaultDescription; }
};

class ConfigFieldId
{
    uintptr_t fieldAddress;

public:
    template <typename T>
    explicit ConfigFieldId(const ConfigField<T>& field) : fieldAddress(reinterpret_cast<std::uintptr_t>(std::addressof(field)))
    {
    }

    friend std::ostream& operator<<(std::ostream& os, const ConfigFieldId& fieldAddress)
    {
        return os << fmt::format("{:#x}", fieldAddress.fieldAddress);
    }

    friend bool operator==(const ConfigFieldId& lhs, const ConfigFieldId& rhs) = default;

    friend struct std::hash<ConfigFieldId>;
};

/// Type-erased view of a ConfigField, addressable by its fully qualified name.
class QualifiedErasedConfigField
{
    QualifiedIdentifier name;
    ConfigFieldId originalFieldAddress;
    std::function<std::expected<ExplicitAny, Exception>(const ConfigLiteral&)> factory;
    std::optional<std::function<std::any()>> defaultSupplier;
    std::string description;
    std::optional<std::string> defaultDescription;

public:
    QualifiedErasedConfigField(
        QualifiedIdentifier name,
        ConfigFieldId originalFieldAddress,
        std::function<std::expected<ExplicitAny, Exception>(const ConfigLiteral&)> factory,
        std::optional<std::function<std::any()>> defaultFactory,
        std::string description,
        std::optional<std::string> defaultDescription)
        : name(std::move(name))
        , originalFieldAddress(originalFieldAddress)
        , factory(std::move(factory))
        , defaultSupplier(std::move(defaultFactory))
        , description(std::move(description))
        , defaultDescription(std::move(defaultDescription))
    {
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    [[nodiscard]] std::string_view getDescription() const { return description; }

    [[nodiscard]] const std::optional<std::string>& getDefaultDescription() const { return defaultDescription; }

    [[nodiscard]] std::expected<ExplicitAny, Exception> apply(const ConfigLiteral& literal) const { return factory(literal); }

    [[nodiscard]] std::function<std::expected<ExplicitAny, Exception>(const ConfigLiteral&)> getFactory() const { return factory; }

    [[nodiscard]] bool hasDefault() const { return defaultSupplier.has_value(); }

    [[nodiscard]] std::any getDefault() const { return defaultSupplier.value()(); }

    [[nodiscard]] std::optional<std::function<std::any()>> getDefaultSupplier() const { return defaultSupplier; }

    [[nodiscard]] ConfigFieldId getOriginalFieldAddress() const { return originalFieldAddress; }

    friend std::ostream& operator<<(std::ostream& os, const QualifiedErasedConfigField& field) { return os << field.name; }
};

/// Combines ConfigFields or other Schema<QualifiedErasedConfigField, Ordered> into one, prefixing all names.
template <typename... T>
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(Identifier prefix, const T&... configComponent)
{
    auto convertField = [&prefix]<typename R>(const ConfigField<R>& field)
    {
        auto defaultSupplier = field.getDefaultSupplier();
        auto defaultDescription = field.getDefaultDescription();
        return std::vector{QualifiedErasedConfigField{
            QualifiedIdentifier::create(prefix, field.getName()),
            ConfigFieldId{field},
            [factory = field.getFactory()](const ConfigLiteral& value)
            { return std::move(factory(value)).transform([](auto val) { return ExplicitAny{std::any(std::move(val))}; }); },
            defaultSupplier.has_value()
                ? std::optional<std::function<std::any()>>{[supplier = std::move(*defaultSupplier)] { return std::any{supplier()}; }}
                : std::nullopt,
            std::string{field.getDescription()},
            defaultDescription.has_value() ? std::optional<std::string>{std::string{*defaultDescription}} : std::nullopt}};
    };

    auto convertSubConfig = [&prefix](const Schema<QualifiedErasedConfigField, Ordered>& subConfig)
    {
        return subConfig
            | std::views::transform(
                   [&prefix](const QualifiedErasedConfigField& subConfigField)
                   {
                       return QualifiedErasedConfigField{
                           QualifiedIdentifier::create(prefix) + subConfigField.getFullyQualifiedName(),
                           subConfigField.getOriginalFieldAddress(),
                           subConfigField.getFactory(),
                           subConfigField.getDefaultSupplier(),
                           std::string{subConfigField.getDescription()},
                           subConfigField.getDefaultDescription()};
                   })
            | std::ranges::to<std::vector>();
    };

    auto convertComponent = [&convertField, &convertSubConfig]<typename C>(const C& component)
    {
        if constexpr (requires(C field) { convertField(field); })
        {
            return convertField(component);
        }
        else if constexpr (requires(C subConfig) { convertSubConfig(subConfig); })
        {
            return convertSubConfig(component);
        }
        else
        {
            static_assert(false, "Missing handler for component type");
        }
    };


    return Schema<QualifiedErasedConfigField, Ordered>{std::views::join(std::array{convertComponent(configComponent)...})};
}

/// Combines ConfigFields or other Schema<QualifiedErasedConfigField, Ordered> into one, without prefixing names.
template <typename First, typename... Rest>
requires(!std::same_as<std::remove_cvref_t<First>, Identifier>)
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(const First& firstComponent, const Rest&... configComponent)
{
    auto convertField = []<typename R>(const ConfigField<R>& field)
    {
        auto defaultSupplier = field.getDefaultSupplier();
        auto defaultDescription = field.getDefaultDescription();
        return std::vector{QualifiedErasedConfigField{
            QualifiedIdentifier::create(field.getName()),
            ConfigFieldId{field},
            [factory = field.getFactory()](const ConfigLiteral& value)
            { return std::move(factory(value)).transform([](auto val) { return ExplicitAny{std::any(std::move(val))}; }); },
            defaultSupplier.has_value()
                ? std::optional<std::function<std::any()>>{[supplier = std::move(*defaultSupplier)] { return std::any{supplier()}; }}
                : std::nullopt,
            std::string{field.getDescription()},
            defaultDescription.has_value() ? std::optional<std::string>{std::string{*defaultDescription}} : std::nullopt}};
    };

    auto convertSubConfig
        = [](const Schema<QualifiedErasedConfigField, Ordered>& subConfig) { return subConfig | std::ranges::to<std::vector>(); };

    auto convertComponent = [&convertField, &convertSubConfig]<typename C>(const C& component)
    {
        if constexpr (requires(C field) { convertField(field); })
        {
            return convertField(component);
        }
        else if constexpr (requires(C subConfig) { convertSubConfig(subConfig); })
        {
            return convertSubConfig(component);
        }
        else
        {
            static_assert(false, "Missing handler for component type");
        }
    };

    return Schema<QualifiedErasedConfigField, Ordered>{
        std::views::join(std::array{convertComponent(firstComponent), convertComponent(configComponent)...})};
}

/// Monadic lowering of a literal's type into a field's type, e.g.
/// tryGetOr<uint64_t>(literal, ...).and_then(downcastConfigValue<uint64_t, uint32_t>).
/// Max can further restrict the range (e.g. 65535 for ports).
template <typename From, typename To, auto Max = std::numeric_limits<To>::max()>
requires std::convertible_to<From, To>
std::expected<To, Exception> downcastConfigValue(From from)
{
    if constexpr (std::is_same_v<From, To>)
    {
        return from;
    }
    else if constexpr (std::integral<From> && std::integral<To>)
    {
        if (std::cmp_less(from, std::numeric_limits<To>::lowest()) || std::cmp_greater(from, Max))
        {
            return std::unexpected{InvalidConfigParameter("Value {} out of range, maximum is: {}", from, Max)};
        }
        return static_cast<To>(from);
    }
    else
    {
        if (from > Max)
        {
            return std::unexpected{InvalidConfigParameter("Value {} out of range, maximum is: {}", from, Max)};
        }
        return static_cast<To>(from);
    }
}

/// Frontend-specific default values for config fields, represented in literals
class ConfigFieldDefault
{
public:
    ConfigFieldDefault(QualifiedIdentifier name, std::function<ConfigLiteral()> supplier)
        : name(std::move(name)), supplier(std::move(supplier))
    {
    }

    ConfigFieldDefault(const std::string_view name, std::function<ConfigLiteral()> supplier)
        : name(QualifiedIdentifier::parse(name)), supplier(std::move(supplier))
    {
    }

    friend bool operator==(const ConfigFieldDefault& lhs, const ConfigFieldDefault& rhs) { return lhs.name == rhs.name; }

    friend std::ostream& operator<<(std::ostream& os, const ConfigFieldDefault& obj)
    {
        return os << fmt::format("ConfigFieldDefault({})", obj.name);
    }

    [[nodiscard]] ConfigLiteral get() const { return supplier(); }

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const { return name; }

    [[nodiscard]] LiteralConfigValue toLiteralConfigValue() const { return LiteralConfigValue{name, get()}; }

private:
    QualifiedIdentifier name;
    std::function<ConfigLiteral()> supplier;
};

/// Frontend-specific transformation of validated instances of ConfigValue (not the final config structs).
struct ConfigFieldTransformation
{
    QualifiedIdentifier name;
    std::function<std::expected<ExplicitAny, Exception>(const ExplicitAny&)> transformation;

    template <typename T>
    static ConfigFieldTransformation create(QualifiedIdentifier name, std::function<T(const T&)> transformation)
    {
        return ConfigFieldTransformation{
            .name = std::move(name),
            .transformation = [transformation = std::move(transformation)](const ExplicitAny& value)
            { return std::expected<ExplicitAny, Exception>{ExplicitAny{std::any{transformation(value.getAs<T>())}}}; }};
    }

    template <typename T>
    static ConfigFieldTransformation
    createWithFail(QualifiedIdentifier name, std::function<std::expected<T, Exception>(const T&)> transformation)
    {
        return ConfigFieldTransformation{
            .name = std::move(name),
            .transformation = [transformation = std::move(transformation)](const ExplicitAny& value)
            { return transformation(value.getAs<T>()).transform([](const auto& result) { return ExplicitAny{std::any{result}}; }); }};
    }

    friend bool operator==(const ConfigFieldTransformation& lhs, const ConfigFieldTransformation& rhs) { return lhs.name == rhs.name; }

    friend std::ostream& operator<<(std::ostream& os, const ConfigFieldTransformation& obj)

    {
        return os << fmt::format("ConfigFieldTransformation({})", obj.name);
    }

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const { return name; }
};
}

template <>
struct std::hash<NES::ConfigFieldDefault>
{
    size_t operator()(const NES::ConfigFieldDefault& obj) const noexcept { return folly::hash::hash_combine(obj.getFullyQualifiedName()); }
};

template <>
struct std::hash<NES::ConfigFieldTransformation>
{
    size_t operator()(const NES::ConfigFieldTransformation& obj) const noexcept { return std::hash<NES::QualifiedIdentifier>{}(obj.name); }
};

template <>
struct std::hash<NES::ConfigFieldId>
{
    size_t operator()(const NES::ConfigFieldId& obj) const noexcept { return obj.fieldAddress; }
};

template <>
struct fmt::formatter<NES::ConfigLiteral> : formatter<std::string_view>
{
    context::iterator format(const NES::ConfigLiteral& literal, format_context& ctx) const;
};

FMT_OSTREAM(NES::LiteralConfigValue);
FMT_OSTREAM(NES::ConfigFieldId);
FMT_OSTREAM(NES::ConfigFieldDefault);
FMT_OSTREAM(NES::ConfigFieldTransformation);
FMT_OSTREAM(NES::QualifiedErasedConfigField);
