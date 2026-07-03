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
#include <concepts>
#include <cstdint>
#include <optional>
#include <ostream>

#include <nameof.hpp>
#include "DataTypes/UnboundField.hpp"
#include "Identifiers/QualifiedIdentifier.hpp"
#include "Schema/Schema.hpp"
#include "Schema/SchemaFwd.hpp"
#include "Util/Logger/Formatter.hpp"

namespace NES
{
using ConfigLiteral = std::variant<std::string, int64_t, uint64_t, double, bool, Schema<UnqualifiedUnboundField, Ordered>>;

template <typename T>
class ConfigField
{
    Identifier name;
    std::type_index type;
    std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory;
    std::optional<std::function<T()>> defaultSupplier;

public:
    ConfigField(std::string name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory)
        : name(Identifier::parse(std::move(name)))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier(std::nullopt)
    {
    }

    ConfigField(
        std::string name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory, std::function<T()> defaultSupplier)
        : name(Identifier::parse(std::move(name)))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier(std::move(defaultSupplier))
    {
    }

    ConfigField(std::string name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory, T defaultValue)
        : name(Identifier::parse(std::move(name)))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier([defaultValue] { return defaultValue; })
    {
    }

    ConfigField(Identifier name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory)
        : name(std::move(name)), type(std::type_index(typeid(T))), factory(std::move(factory)), defaultSupplier(std::nullopt)
    {
    }

    ConfigField(
        Identifier name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory, std::function<T()> defaultSupplier)
        : name(std::move(name)), type(std::type_index(typeid(T))), factory(std::move(factory)), defaultSupplier(std::move(defaultSupplier))
    {
    }

    ConfigField(Identifier name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory, T defaultValue)
        : name(std::move(name))
        , type(std::type_index(typeid(T)))
        , factory(std::move(factory))
        , defaultSupplier([defaultValue] { return defaultValue; })
    {
    }

    [[nodiscard]] Identifier getName() const { return name; }

    [[nodiscard]] std::type_index getType() const { return type; }

    [[nodiscard]] std::function<std::expected<T, Exception>(const ConfigLiteral&)> getFactory() const { return factory; }

    [[nodiscard]] std::optional<std::function<T()>> getDefaultSupplier() const { return defaultSupplier; }
};

/// Wraps std::any for use as the value type of std::expected. std::expected<std::any, E> itself is ill-formed with
/// clang + libstdc++: std::any is constructible from anything, including the expected itself, so the constraints on
/// expected's converting constructors (__cons_from_expected) recursively depend on themselves. Restricting the
/// constructor to exactly std::any makes this type not constructible from the surrounding expected, breaking the cycle.
/// The same_as constraint is load-bearing: a plain std::any parameter would accept the expected via std::any's
/// implicit converting constructor, and checking that conversion re-enters the same constraint recursion.
class ErasedConfigValue
{
    std::any value;

public:
    template <std::same_as<std::any> A>
    explicit ErasedConfigValue(A value) : value(std::move(value))
    {
    }

    [[nodiscard]] const std::any& getValue() const { return value; }
};

class QualifiedErasedConfigField
{
    QualifiedIdentifier name;
    std::function<std::expected<ErasedConfigValue, Exception>(const ConfigLiteral&)> factory;
    std::optional<std::function<std::any()>> defaultSupplier;

public:
    QualifiedErasedConfigField(
        QualifiedIdentifier name,
        std::function<std::expected<ErasedConfigValue, Exception>(const ConfigLiteral&)> factory,
        std::optional<std::function<std::any()>> defaultFactory)
        : name(std::move(name)), factory(std::move(factory)), defaultSupplier(std::move(defaultFactory))
    {
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    [[nodiscard]] std::expected<ErasedConfigValue, Exception> apply(const ConfigLiteral& literal) const { return factory(literal); }

    [[nodiscard]] bool hasDefault() const { return defaultSupplier.has_value(); }

    [[nodiscard]] std::any getDefault() const { return defaultSupplier.value()(); }

    friend std::ostream& operator<<(std::ostream& os, const QualifiedErasedConfigField& field) { return os << field.name; }
};

template <typename... T>
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(Identifier prefix, ConfigField<T>... fields)
{
    auto convertField = [&prefix]<typename R>(ConfigField<R> field)
    {
        return QualifiedErasedConfigField{
            QualifiedIdentifier::create(prefix, field.getName()),
            [factory = field.getFactory()](const ConfigLiteral& value)
            { return std::move(factory(value)).transform([](auto val) { return ErasedConfigValue{std::any(std::move(val))}; }); },
            field.getDefaultSupplier()};
    };
    return Schema<QualifiedErasedConfigField, Ordered>{convertField(fields)...};
}

template <typename T>
std::function<Exception()> expectedType()
{
    return [] { return InvalidConfigParameter("Expected type: {}", NAMEOF_TYPE(T)); };
}

}

FMT_OSTREAM(NES::QualifiedErasedConfigField);