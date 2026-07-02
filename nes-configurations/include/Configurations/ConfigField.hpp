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
#include <cstdint>
#include <optional>

#include "DataTypes/UnboundField.hpp"
#include "Identifiers/QualifiedIdentifier.hpp"
#include "Schema/Schema.hpp"
#include "Schema/SchemaFwd.hpp"

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
    ConfigField(Identifier name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory)
        : name(std::move(name)), type(std::type_index(typeid(T))), factory(std::move(factory)), defaultSupplier(std::nullopt)
    {
    }

    ConfigField(Identifier name, std::function<std::expected<T, Exception>(const ConfigLiteral&)> factory, std::function<T()> defaultSupplier)
        : name(std::move(name)), type(std::type_index(typeid(T))), factory(std::move(factory)), defaultSupplier(std::move(defaultSupplier))
    {
    }

    [[nodiscard]] Identifier getName() const { return name; }

    [[nodiscard]] std::type_index getType() const { return type; }

    [[nodiscard]] std::function<std::expected<T, Exception>(const ConfigLiteral&)> getFactory() const { return factory; }
    [[nodiscard]] std::optional<std::function<T()>> getDefaultSupplier() const { return defaultSupplier; }
};

class QualifiedErasedConfigField
{
    QualifiedIdentifier name;
    std::function<std::expected<std::any, Exception>(const ConfigLiteral&)> factory;
    std::optional<std::function<std::any()>> defaultSupplier;

public:
    QualifiedErasedConfigField(QualifiedIdentifier name, std::function<std::expected<std::any, Exception>(const ConfigLiteral&)> factory, std::optional<std::function<std::any()>> defaultFactory)
        : name(std::move(name)), factory(std::move(factory)), defaultSupplier(std::move(defaultFactory))
    {
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    [[nodiscard]] std::expected<std::any, Exception> apply(const ConfigLiteral& literal) const { return factory(literal); }
    [[nodiscard]] bool hasDefault() const { return defaultSupplier.has_value(); }
    [[nodiscard]] std::any getDefault() const { return defaultSupplier.value()(); }
};

template <typename... T>
Schema<QualifiedErasedConfigField, Ordered> createConfigSchema(Identifier prefix, ConfigField<T>... fields)
{
    auto convertField = [&prefix]<typename R>(ConfigField<R> field)
    {
        return QualifiedErasedConfigField{
            prefix + field.name,
            [factory = field.getFactory()](const ConfigLiteral& value)
            { return std::move(factory(value)).transform([](auto val) { return std::any(std::move(val)); }); },
            field.getDefaultSupplier()
        };
    };
    return Schema<QualifiedErasedConfigField, Ordered>{convertField(fields)...};
}

}