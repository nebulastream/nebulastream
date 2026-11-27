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

#include <SourceConfigSchemaRegistry.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/ScalarTypes.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Types/NESType.hpp>
#include <Util/Optional.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{

namespace
{
std::expected<ConfigLiteral, Exception> nesValueToConfigLiteral(const NESValue& val)
{
    if (const auto* cfgLit = val.tryGetAs<ConfigLiteral>())
    {
        return *cfgLit;
    }
    if (const auto* schema = val.tryGetAs<Schema<UnqualifiedUnboundField, Ordered>>())
    {
        return ConfigLiteral{*schema};
    }
    if (val.tryGetAs<std::monostate>() != nullptr)
    {
        return ConfigLiteral{std::monostate{}};
    }
    if (const auto* s = val.tryGetAs<std::string>())
    {
        return ConfigLiteral{*s};
    }
    if (const auto* c = val.tryGetAs<char>())
    {
        return ConfigLiteral{std::string{1, *c}};
    }
    if (const auto* b = val.tryGetAs<bool>())
    {
        return ConfigLiteral{*b};
    }
    if (val.getType().hasTypeTrait<IntegerTypeTrait>())
    {
        return tryExtractSignedInteger(val).transform([](const int64_t v) { return ConfigLiteral{v}; });
    }
    if (val.getType().hasTypeTrait<FloatTypeTrait>())
    {
        return tryExtractFloatingPoint(val).transform([](const double v) { return ConfigLiteral{v}; });
    }
    if (const auto signedInt = tryExtractSignedInteger(val); signedInt.has_value())
    {
        return ConfigLiteral{*signedInt};
    }
    if (const auto flt = tryExtractFloatingPoint(val); flt.has_value())
    {
        return ConfigLiteral{*flt};
    }
    return std::unexpected{InvalidQuerySyntax("Invalid config argument value passed to source constructor")};
}
}

std::expected<
    std::tuple<GeneralSourceConfig, PluginSourceConfiguration, InputFormatterDescriptor, Schema<UnqualifiedUnboundField, Ordered>>,
    Exception>
BoundAnonymousSource::resolve(
    const Schema<ConfigFieldDefault, Ordered>& configDefaults,
    const Schema<ConfigFieldTransformation, Unordered>& configTransformations) const
{
    const auto peekInputFormatter = [&]() -> std::optional<LiteralConfigValue>
    {
        static const auto fqType = QualifiedIdentifier::parse("INPUT_FORMATTER.TYPE");
        static const auto shortType = QualifiedIdentifier::parse("TYPE");
        for (const auto& opt : configOptions)
        {
            if (opt.getFullyQualifiedName() == fqType || opt.getFullyQualifiedName() == shortType)
            {
                return opt;
            }
        }
        return std::nullopt;
    };

    const auto inputFormatterTypeExp
        = optionalToExpected(
              peekInputFormatter()
                  .or_else(
                      [&]
                      {
                          return configDefaults.getFieldByName(QualifiedIdentifier::parse("INPUT_FORMATTER.TYPE"))
                              .transform(
                                  [](const ConfigFieldDefault& defaultInputFormatter)
                                  {
                                      return LiteralConfigValue{
                                          defaultInputFormatter.getFullyQualifiedName(), defaultInputFormatter.get()};
                                  });
                      }),
              [] { return Exception{InvalidQuerySyntax("INPUT_FORMATTER.TYPE must be specified")}; })
              .and_then(
                  [](const LiteralConfigValue& value)
                  { return tryGetOr<std::string>(value.getValue(), expectedType<std::string>()).and_then(Identifier::tryParse); });
    if (!inputFormatterTypeExp.has_value())
    {
        return std::unexpected{inputFormatterTypeExp.error()};
    }

    auto resolvedExp
        = SourceCatalog::getConfigSchema(sourceType, *inputFormatterTypeExp)
              .transform([&](const SourceConfigSchema& schema)
                         { return schema.withConfigDefaults(configDefaults).withConfigTransformations(configTransformations); })
              .and_then([&](const SourceConfigSchema& schema) { return schema.resolveConfigs(configOptions); });
    if (!resolvedExp.has_value())
    {
        return std::unexpected{resolvedExp.error()};
    }

    auto& [generalConfig, pluginConfig, inputFormatter, schemaOpt] = *resolvedExp;
    if (!schemaOpt.has_value())
    {
        return std::unexpected{InvalidConfigParameter("Anonymous source '{}' requires SOURCE.SCHEMA to be specified", sourceType)};
    }
    return std::make_tuple(
        std::move(generalConfig), std::move(pluginConfig), std::move(inputFormatter), std::move(*schemaOpt));
}

SourceConfigSchemaRegistry& SourceConfigSchemaRegistry::instance()
{
    static SourceConfigSchemaRegistry inst;
    return inst;
}

NESTypeClass SourceConfigSchemaRegistry::sourceTypeClass()
{
    return NESTypeClass::forTypeTrait<SourceTypeTrait>();
}

std::vector<NamedNESParametrizedTypeClass> SourceConfigSchemaRegistry::getParametrizedTypeClasses()
{
    return {
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("Source"), sourceTypeClass()),
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("SourceConfigSchema"), sourceTypeClass())};
}

namespace
{
struct AnonymousSourceNESType
{
    Identifier sourceType;
    SourceConfigSchemaProviderFn schemaProvider;

    bool operator==(const AnonymousSourceNESType& other) const { return sourceType == other.sourceType; }

    [[nodiscard]] std::expected<BoundAnonymousSource, Exception> construct(const NESValueParameters& params) const
    {
        std::vector<LiteralConfigValue> options;
        if (!params.empty())
        {
            const auto* namedSchema = params.tryGetNamedSchema();
            if (namedSchema == nullptr)
            {
                return std::unexpected{InvalidQuerySyntax(
                    "Source constructor '{}' requires named config arguments ('val' AS GROUP.KEY)", sourceType)};
            }
            options.reserve(namedSchema->size());
            for (const auto& namedArg : *namedSchema)
            {
                auto litExp = nesValueToConfigLiteral(namedArg.getValue());
                if (!litExp.has_value())
                {
                    return std::unexpected{std::move(litExp).error()};
                }
                options.emplace_back(namedArg.getFullyQualifiedName(), std::move(*litExp));
            }
        }
        try
        {
            return BoundAnonymousSource{
                .sourceType = sourceType,
                .configOptions = createConfigLiteralSchema(std::move(options))};
        }
        catch (const Exception& ex)
        {
            return std::unexpected{ex};
        }
    }
};

struct SourceConfigSchemaFunctor
{
    Identifier sourceType;
    SourceConfigSchemaProviderFn schemaProvider;

    [[nodiscard]] const SourceConfigSchemaProviderFn& getRegistryEntry() const noexcept { return schemaProvider; }
    [[nodiscard]] static ExpectedNESTypeArguments getExpectedArguments() { return ExpectedNESTypeArguments{}; }
    bool operator==(const SourceConfigSchemaFunctor& other) const { return sourceType == other.sourceType; }

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters&) const
    {
        return NESType{AnonymousSourceNESType{.sourceType = sourceType, .schemaProvider = schemaProvider}}
            .withTypeTrait(SourceTypeTrait{.sourceType = sourceType});
    }
};
}

NESFunctor makeSourceConfigSchemaFunctor(const std::string_view sourceTypeName, SourceConfigSchemaProviderFn provider)
{
    return NESFunctor{SourceConfigSchemaFunctor{
        .sourceType = Identifier::parse(std::string{sourceTypeName}),
        .schemaProvider = std::move(provider)}};
}

}
