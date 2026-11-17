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

#include <Decoders/DecoderChain.hpp>

#include <algorithm>
#include <expected>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Decoders/Decoder.hpp>
#include <Decoders/DecoderStage.hpp>
#include <Decoders/DecoderStageDescriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <DecoderStageConfigRegistry.hpp>
#include <DecoderStageConfigSchemaRegistry.hpp>
#include <DecoderStageRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES::DecoderChain
{

namespace
{
Exception unknownStage(const Identifier& stageType)
{
    return UnknownDecoderType("The decoder stage '{}' is not registered. If it is a plugin, make sure you activated it.", stageType);
}
}

std::expected<void, Exception> validate(const std::vector<Identifier>& stageTypes)
{
    if (stageTypes.empty())
    {
        return std::unexpected{InvalidConfigParameter("A decoder needs at least one stage")};
    }
    for (auto stage = stageTypes.begin(); stage != stageTypes.end(); ++stage)
    {
        if (not DecoderStageRegistry::instance().contains(stage->asCanonicalString()))
        {
            return std::unexpected{unknownStage(*stage)};
        }
        if (std::find(stageTypes.begin(), stage, *stage) != stage)
        {
            return std::unexpected{InvalidConfigParameter("The decoder stage '{}' appears twice in the chain", *stage)};
        }
    }
    return {};
}

std::expected<Schema<QualifiedErasedConfigField, Ordered>, Exception> configSchema(const std::vector<Identifier>& stageTypes)
{
    std::vector<QualifiedErasedConfigField> fields;
    for (const auto& stageType : stageTypes)
    {
        auto schema = DecoderStageConfigSchemaRegistry::instance().getSchema(stageType.asCanonicalString());
        if (not schema)
        {
            return std::unexpected{unknownStage(stageType)};
        }
        std::ranges::copy(*schema, std::back_inserter(fields));
    }
    return Schema<QualifiedErasedConfigField, Ordered>{std::move(fields)};
}

std::expected<std::vector<DecoderStageDescriptor>, Exception>
instantiate(const std::vector<Identifier>& stageTypes, const InstantiatedConfig& config)
{
    std::vector<DecoderStageDescriptor> descriptors;
    descriptors.reserve(stageTypes.size());
    for (const auto& stageType : stageTypes)
    {
        const auto entry = DecoderStageConfigRegistry::instance().find(stageType.asCanonicalString());
        if (not entry)
        {
            return std::unexpected{unknownStage(stageType)};
        }
        auto stageConfig = entry->instantiate(config);
        if (not stageConfig.has_value())
        {
            return std::unexpected{stageConfig.error()};
        }
        descriptors.emplace_back(stageType, std::move(stageConfig).value());
    }
    return descriptors;
}

std::expected<std::vector<DecoderStageDescriptor>, Exception>
createStages(const std::vector<Identifier>& stageTypes, const Schema<LiteralConfigValue, Ordered>& options)
{
    if (auto valid = validate(stageTypes); not valid.has_value())
    {
        return std::unexpected{valid.error()};
    }
    auto schema = configSchema(stageTypes);
    if (not schema.has_value())
    {
        return std::unexpected{schema.error()};
    }
    auto [resolved, errors] = resolveConfig(options, *schema);
    if (not errors.empty())
    {
        return std::unexpected{InvalidConfigParameter("Invalid decoder options: {}", errors)};
    }
    return instantiate(stageTypes, InstantiatedConfig{resolved});
}

std::unique_ptr<Decoder> createDecoder(const std::vector<DecoderStageDescriptor>& stages)
{
    if (stages.empty())
    {
        return nullptr;
    }
    std::vector<std::unique_ptr<DecoderStage>> instances;
    instances.reserve(stages.size());
    for (const auto& stage : stages)
    {
        const auto factory = DecoderStageRegistry::instance().find(stage.getStageType().asCanonicalString());
        if (not factory)
        {
            throw unknownStage(stage.getStageType());
        }
        instances.push_back((*factory)(stage));
    }
    return std::make_unique<Decoder>(std::move(instances));
}

}
