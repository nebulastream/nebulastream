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
#include <DataTypes/DataType.hpp>
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
struct SourceTypeDescriptor
{
    Identifier sourceType;
    SourceConfigSchemaProviderFn schemaProvider;
    bool operator==(const SourceTypeDescriptor& other) const { return sourceType == other.sourceType; }
};

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
    if (const auto* scalar = val.tryGetAs<ScalarLiteralValue>())
    {
        return std::visit(
            Overloaded{
                [](const uint64_t unsignedInt) -> std::expected<ConfigLiteral, Exception>
                {
                    if (!std::in_range<int64_t>(unsignedInt))
                    {
                        return std::unexpected{
                            InvalidQuerySyntax("Integer literal {} exceeds the supported signed integer range", unsignedInt)};
                    }
                    return ConfigLiteral{static_cast<int64_t>(unsignedInt)};
                },
                [](const auto& other) -> std::expected<ConfigLiteral, Exception> { return ConfigLiteral{other}; }},
            *scalar);
    }
    if (const auto* s = val.tryGetAs<std::string>())
    {
        return ConfigLiteral{*s};
    }
    if (const auto* i = val.tryGetAs<int64_t>())
    {
        return ConfigLiteral{*i};
    }
    if (const auto* b = val.tryGetAs<bool>())
    {
        return ConfigLiteral{*b};
    }
    if (const auto* d = val.tryGetAs<double>())
    {
        return ConfigLiteral{*d};
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
    const auto inputFormatterTypeExp
        = optionalToExpected(
              configOptions.getFieldByName(QualifiedIdentifier::parse("INPUT_FORMATTER.TYPE"))
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
    return NESTypeClass::forTrait<SourceTypeTrait>();
}

std::vector<NamedNESParametrizedTypeClass> SourceConfigSchemaRegistry::getParametrizedTypeClasses()
{
    return {
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("Source"), sourceTypeClass()),
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("SourceConfigSchema"), sourceTypeClass())};
}

NESFunctor makeSourceConfigSchemaFunctor(const std::string_view sourceTypeName, SourceConfigSchemaProviderFn provider)
{
    const std::string nameCopy{sourceTypeName};
    return NESFunctor::nullary<SourceConfigSchemaProviderFn>(
        std::move(provider),
        [nameCopy](const SourceConfigSchemaProviderFn& schemaFn) -> std::expected<NESType, Exception>
        {
            const auto sourceId = Identifier::parse(nameCopy);
            return NESType::create<SourceTypeDescriptor, BoundAnonymousSource>(
                       SourceTypeDescriptor{.sourceType = sourceId, .schemaProvider = schemaFn},
                       [](const SourceTypeDescriptor& desc, const NESValueParameters& params)
                           -> std::expected<BoundAnonymousSource, Exception>
                       {
                           std::vector<LiteralConfigValue> options;
                           if (!params.empty())
                           {
                               const auto* namedSchema = params.tryGetNamedSchema();
                               if (namedSchema == nullptr)
                               {
                                   return std::unexpected{InvalidQuerySyntax(
                                       "Source constructor '{}' requires named config arguments ('val' AS GROUP.KEY)",
                                       desc.sourceType)};
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
                           return BoundAnonymousSource{
                               .sourceType = desc.sourceType,
                               .configOptions = Schema<LiteralConfigValue, Ordered>{std::move(options)}};
                       })
                .withTrait(SourceTypeTrait{.sourceType = sourceId});
        });
}

}
