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

#include <SinkConfigSchemaRegistry.hpp>

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
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Types/NESType.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
struct SinkTypeDescriptor
{
    Identifier sinkType;
    SinkConfigSchemaProviderFn schemaProvider;
    bool operator==(const SinkTypeDescriptor& other) const { return sinkType == other.sinkType; }
};

std::expected<ConfigLiteral, Exception> nesValueToSinkConfigLiteral(const NESValue& val)
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
    return std::unexpected{InvalidQuerySyntax("Invalid config argument value passed to sink constructor")};
}
}

std::expected<std::tuple<GeneralSinkConfig, AnonymousSinkSchema, PluginSinkConfiguration, OutputFormatterDescriptor>, Exception>
BoundAnonymousSink::resolve(
    const Schema<ConfigFieldDefault, Ordered>& configDefaults,
    const Schema<ConfigFieldTransformation, Unordered>& configTransformations) const
{
    const auto typeLiteral
        = configOptions.getFieldByName(QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"))
              .or_else(
                  [&]
                  {
                      return configDefaults.getFieldByName(QualifiedIdentifier::parse("OUTPUT_FORMATTER.TYPE"))
                          .transform([](const ConfigFieldDefault& defaultFormatter)
                                     { return LiteralConfigValue{defaultFormatter.getFullyQualifiedName(), defaultFormatter.get()}; });
                  });
    auto outputFormatterTypeExp = [&]() -> std::expected<Identifier, Exception>
    {
        if (!typeLiteral.has_value())
        {
            return Identifier::parse("NATIVE");
        }
        return tryGetOr<std::string>(typeLiteral->getValue(), expectedType<std::string>()).and_then(Identifier::tryParse);
    }();
    if (!outputFormatterTypeExp.has_value())
    {
        return std::unexpected{outputFormatterTypeExp.error()};
    }

    return SinkCatalog::getConfigSchema(sinkType, *outputFormatterTypeExp)
        .transform([&](const SinkConfigSchema& schema)
                   { return schema.withConfigDefaults(configDefaults).withConfigTransformations(configTransformations); })
        .and_then([&](const SinkConfigSchema& schema) { return schema.resolveConfigs(configOptions); });
}

SinkConfigSchemaRegistry& SinkConfigSchemaRegistry::instance()
{
    static SinkConfigSchemaRegistry inst;
    return inst;
}

NESTypeClass SinkConfigSchemaRegistry::sinkTypeClass()
{
    return NESTypeClass::forTrait<SinkTypeTrait>();
}

std::vector<NamedNESParametrizedTypeClass> SinkConfigSchemaRegistry::getParametrizedTypeClasses()
{
    return {
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("Sink"), sinkTypeClass()),
        NamedNESParametrizedTypeClass::constant(QualifiedIdentifier::parse("SinkConfigSchema"), sinkTypeClass())};
}

NESFunctor makeSinkConfigSchemaFunctor(const std::string_view sinkTypeName, SinkConfigSchemaProviderFn provider)
{
    const std::string nameCopy{sinkTypeName};
    return NESFunctor::nullary<SinkConfigSchemaProviderFn>(
        std::move(provider),
        [nameCopy](const SinkConfigSchemaProviderFn& schemaFn) -> std::expected<NESType, Exception>
        {
            const auto sinkId = Identifier::parse(nameCopy);
            return NESType::create<SinkTypeDescriptor, BoundAnonymousSink>(
                       SinkTypeDescriptor{.sinkType = sinkId, .schemaProvider = schemaFn},
                       [](const SinkTypeDescriptor& desc, const NESValueParameters& params)
                           -> std::expected<BoundAnonymousSink, Exception>
                       {
                           std::vector<LiteralConfigValue> options;
                           if (!params.empty())
                           {
                               const auto* namedSchema = params.tryGetNamedSchema();
                               if (namedSchema == nullptr)
                               {
                                   return std::unexpected{InvalidQuerySyntax(
                                       "Sink constructor '{}' requires named config arguments ('val' AS GROUP.KEY)", desc.sinkType)};
                               }
                               options.reserve(namedSchema->size());
                               for (const auto& namedArg : *namedSchema)
                               {
                                   auto litExp = nesValueToSinkConfigLiteral(namedArg.getValue());
                                   if (!litExp.has_value())
                                   {
                                       return std::unexpected{std::move(litExp).error()};
                                   }
                                   options.emplace_back(namedArg.getFullyQualifiedName(), std::move(*litExp));
                               }
                           }
                           return BoundAnonymousSink{
                               .sinkType = desc.sinkType,
                               .configOptions = Schema<LiteralConfigValue, Ordered>{std::move(options)}};
                       })
                .withTrait(SinkTypeTrait{.sinkType = sinkId});
        });
}

}
