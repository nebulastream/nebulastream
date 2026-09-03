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
#include <utility>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Variant.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <nameof.hpp>

namespace NES
{

template <typename T>
std::function<Exception()> expectedType()
{
    return [] { return InvalidConfigParameter("Expected type: {}", NAMEOF_TYPE(T)); };
}

/// Frontends type integer-looking input as int64_t (see ConfigLiteral), so fields with a floating
/// point type accept both fractional and integer literals; the integer is widened to double.
std::expected<double, Exception> tryGetDoubleOrInt(const ConfigLiteral& literal, const std::function<Exception()>& orElse);

/// Declares a single typed config parameter: its name, how to instantiate it from a literal the
/// frontend passed, and (optionally) its default. The field type T is arbitrary — it does not
/// need to be serializable, because serialization of catalog objects goes through the
/// source-defined config struct, not through individual config fields.
/// IMPORTANT: Only use this class in .cpp or if used in a header mark the instances as inline.
/// Otherwise, each TU that includes the header will result in distinct instances.
template <typename T>
class ConfigField
{
    Identifier name;
    std::string description;
    std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory;
    std::optional<std::function<T()>> defaultSupplier;
    std::optional<std::string> defaultDescription;

public:
    ConfigField(Identifier name, std::string description, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory)
        : name(std::move(name))
        , description(std::move(description))
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
        , factory(std::move(factory))
        , defaultSupplier([defaultValue] { return defaultValue; })
        , defaultDescription(std::move(defaultDescription))
    {
    }

    ConfigField(Identifier name, std::string description)
    requires isConfigLiteral<T>
        : name(std::move(name))
        , description(std::move(description))
        , factory([](const ConfigLiteral& literal) { return tryGetOr<T>(literal, expectedType<T>()); })
        , defaultSupplier(std::nullopt)
        , defaultDescription(std::nullopt)
    {
    }

    ConfigField(Identifier name, std::string description, T defaultValue)
    requires isConfigLiteral<T>
        : name(std::move(name))
        , description(std::move(description))
        , factory([](const ConfigLiteral& literal) { return tryGetOr<T>(literal, expectedType<T>()); })
        , defaultSupplier([defaultValue] { return defaultValue; })
        , defaultDescription(fmt::format("{}", defaultValue))
    {
    }

    /// Delete the copy and move ctors since we use the memory address as an ID.
    /// Even with another ID implementation, we should avoid the question of "If we copy a config field, is it still the same field?"
    ConfigField(const ConfigField& other) = delete;
    ConfigField(ConfigField&& other) noexcept = delete;
    ConfigField& operator=(const ConfigField& other) = delete;
    ConfigField& operator=(ConfigField&& other) noexcept = delete;
    ~ConfigField() = default;

    [[nodiscard]] const Identifier& getName() const { return name; }

    [[nodiscard]] const std::function<std::expected<T, Exception>(const ConfigLiteral&)>& getFactory() const { return factory; }

    [[nodiscard]] const std::optional<std::function<T()>>& getDefaultSupplier() const { return defaultSupplier; }

    [[nodiscard]] std::string_view getDescription() const { return description; }

    [[nodiscard]] std::optional<std::string_view> getDefaultDescription() const { return defaultDescription; }
};

/// Each INSTANCE of a ConfigField gets its own ID (currently implemented with the memory address).
/// That means that a ConfigField declared in a header without the inline specifier will be instantiated separately in every TU that includes it.
class ConfigFieldId
{
    uintptr_t fieldAddress;

public:
    template <typename T>
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - a field instance's identity is its address
    explicit ConfigFieldId(const ConfigField<T>& field) : fieldAddress(reinterpret_cast<std::uintptr_t>(std::addressof(field)))
    {
    }

    friend std::ostream& operator<<(std::ostream& os, const ConfigFieldId& fieldAddress);

    friend bool operator==(const ConfigFieldId& lhs, const ConfigFieldId& rhs);

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
        std::optional<std::function<std::any()>> defaultSupplier,
        std::string description,
        std::optional<std::string> defaultDescription);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;

    [[nodiscard]] std::string_view getDescription() const;

    [[nodiscard]] const std::optional<std::string>& getDefaultDescription() const;

    [[nodiscard]] std::expected<ExplicitAny, Exception> apply(const ConfigLiteral& literal) const;

    [[nodiscard]] const std::function<std::expected<ExplicitAny, Exception>(const ConfigLiteral&)>& getFactory() const;

    [[nodiscard]] bool hasDefault() const;

    [[nodiscard]] std::any getDefault() const;

    [[nodiscard]] const std::optional<std::function<std::any()>>& getDefaultSupplier() const;

    [[nodiscard]] const ConfigFieldId& getOriginalFieldAddress() const;

    friend std::ostream& operator<<(std::ostream& os, const QualifiedErasedConfigField& field);
};

template <typename T>
constexpr bool isConfigField = false;
template <typename R>
constexpr bool isConfigField<ConfigField<R>> = true;

template <typename C>
concept ConfigSchemaComponent
    = isConfigField<std::remove_cvref_t<C>> || std::same_as<std::remove_cvref_t<C>, Schema<QualifiedErasedConfigField, Ordered>>;

namespace detail
{
template <ConfigSchemaComponent... T>
Schema<QualifiedErasedConfigField, Ordered>
createConfigSchema(const std::function<QualifiedIdentifier(const QualifiedIdentifier&)>& nameModifier, const T&... configComponent)
{
    auto convertField = [&nameModifier]<typename R>(const ConfigField<R>& field)
    {
        auto defaultSupplier = field.getDefaultSupplier();
        auto defaultDescription = field.getDefaultDescription();
        return std::vector{QualifiedErasedConfigField{
            nameModifier(QualifiedIdentifier::create(field.getName())),
            ConfigFieldId{field},
            [factory = field.getFactory()](const ConfigLiteral& value)
            { return std::move(factory(value)).transform([](auto val) { return ExplicitAny{std::any(std::move(val))}; }); },
            defaultSupplier.transform([](std::function<R()> supplier)
                                      { return [supplier = std::move(supplier)] { return std::any{supplier()}; }; }),
            std::string{field.getDescription()},
            defaultDescription.transform([](const std::string_view description) { return std::string{description}; })}};
    };

    auto convertSubConfig = [&nameModifier](const Schema<QualifiedErasedConfigField, Ordered>& subConfig)
    {
        return subConfig
            | std::views::transform(
                   [&nameModifier](const QualifiedErasedConfigField& subConfigField)
                   {
                       return QualifiedErasedConfigField{
                           nameModifier(subConfigField.getFullyQualifiedName()),
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
}

/// Combines ConfigFields or other Schema<QualifiedErasedConfigField, Ordered> into one, prefixing all names.
/// Returned object will respect the order of the provided configComponents.
/// Keep in mind that any schemas of config fields will be flattened.
template <ConfigSchemaComponent... T>
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(Identifier prefix, const T&... configComponent)
{
    return detail::createConfigSchema(
        [&prefix](const QualifiedIdentifier& name) { return QualifiedIdentifier::create(prefix) + name; }, configComponent...);
}

/// Combines ConfigFields or other Schema<QualifiedErasedConfigField, Ordered> into one, without prefixing names.
template <ConfigSchemaComponent First, ConfigSchemaComponent... Rest>
requires(!std::same_as<std::remove_cvref_t<First>, Identifier>)
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(const First& firstComponent, const Rest&... configComponent)
{
    return detail::createConfigSchema([](const QualifiedIdentifier& name) { return name; }, firstComponent, configComponent...);
}

template <typename T>
concept RvalueConfigField = isConfigField<std::remove_cvref_t<T>> && !std::is_lvalue_reference_v<T>;

template <ConfigSchemaComponent... T>
requires(RvalueConfigField<T &&> || ...)
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(Identifier prefix, T&&...) = delete;

template <ConfigSchemaComponent... T>
requires(RvalueConfigField<T &&> || ...)
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(T&&...) = delete;

/// Frontends produce only int64_t and double, but fields usually want a narrower type. This
/// converts and rejects values outside the target's range. Max and Min restricts it further, for
/// example 65535 for a port:
/// tryGetOr<int64_t>(literal, expectedType<uint32_t>()).and_then(downcastConfigValue<int64_t, uint32_t, 65535>)
template <typename From, typename To, auto Max = std::numeric_limits<To>::max(), auto Min = std::numeric_limits<From>::lowest()>
requires std::convertible_to<From, To>
std::expected<To, Exception> narrowConfigValue(From from)
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
        if (from < Min)
        {
            return std::unexpected{InvalidConfigParameter("Value {} out of range, minimum is: {}", from, Min)};
        }
        return static_cast<To>(from);
    }
}

/// Frontend-specific default values for config fields, represented in literals
class ConfigFieldDefault
{
public:
    ConfigFieldDefault(QualifiedIdentifier name, std::function<ConfigLiteral()> supplier);

    ConfigFieldDefault(QualifiedIdentifier name, ConfigLiteral value);

    friend bool operator==(const ConfigFieldDefault& lhs, const ConfigFieldDefault& rhs);

    friend std::ostream& operator<<(std::ostream& os, const ConfigFieldDefault& obj);

    [[nodiscard]] ConfigLiteral get() const;

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;

private:
    QualifiedIdentifier name;
    std::function<ConfigLiteral()> supplier;
};

/// Frontend-specific transformation of validated instances of ConfigValue (not the final config structs).
class ConfigFieldTransformation
{
public:
    template <typename T>
    ConfigFieldTransformation(QualifiedIdentifier name, std::function<std::expected<T, Exception>(const T&)> transformation)
        : name(std::move(name))
        , transformation(
              [transformation = std::move(transformation)](const ExplicitAny& value)
              { return transformation(value.getAs<T>()).transform([](const auto& result) { return ExplicitAny{std::any{result}}; }); })
    {
    }

    friend bool operator==(const ConfigFieldTransformation& lhs, const ConfigFieldTransformation& rhs);

    friend std::ostream& operator<<(std::ostream& os, const ConfigFieldTransformation& obj);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;

    [[nodiscard]] const std::function<std::expected<ExplicitAny, Exception>(const ExplicitAny&)>& getTransformation() const;

private:
    QualifiedIdentifier name;
    std::function<std::expected<ExplicitAny, Exception>(const ExplicitAny&)> transformation;
    friend struct std::hash<ConfigFieldTransformation>;
};
}

template <>
struct std::hash<NES::ConfigFieldDefault>
{
    size_t operator()(const NES::ConfigFieldDefault& obj) const noexcept;
};

template <>
struct std::hash<NES::ConfigFieldTransformation>
{
    size_t operator()(const NES::ConfigFieldTransformation& obj) const noexcept;
};

template <>
struct std::hash<NES::ConfigFieldId>
{
    size_t operator()(const NES::ConfigFieldId& obj) const noexcept;
};

template <>
struct fmt::formatter<NES::ConfigLiteral> : formatter<std::string_view>
{
    context::iterator format(const NES::ConfigLiteral& literal, format_context& ctx) const;
};

FMT_OSTREAM(NES::ConfigFieldId);
FMT_OSTREAM(NES::ConfigFieldDefault);
FMT_OSTREAM(NES::ConfigFieldTransformation);
FMT_OSTREAM(NES::QualifiedErasedConfigField);
