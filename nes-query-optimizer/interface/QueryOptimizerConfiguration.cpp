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

#include <QueryOptimizerConfiguration.hpp>

#include <expected>
#include <string>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerNetworkConfiguration.hpp>

namespace NES
{

namespace
{

/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<StreamJoinStrategy> JOIN_STRATEGY{
    Identifier::parse("join_strategy"),
    "Join Strategy"
    "[NESTED_LOOP_JOIN|HASH_JOIN|OPTIMIZER_CHOOSES].",
    [](const ConfigLiteral& literal)
    {
        return tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](std::string&& value) -> std::expected<StreamJoinStrategy, Exception>
                {
                    if (const auto parsed = EnumWrapper{value}.asEnum<StreamJoinStrategy>())
                    {
                        return *parsed;
                    }
                    return std::unexpected{InvalidConfigParameter(
                        "Invalid join strategy, must be NESTED_LOOP_JOIN, HASH_JOIN or OPTIMIZER_CHOOSES: {}", value)};
                });
    },
    StreamJoinStrategy::OPTIMIZER_CHOOSES,
    "OPTIMIZER_CHOOSES"};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> QueryOptimizerConfiguration::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("optimizer"), JOIN_STRATEGY, QueryOptimizerNetworkConfiguration::getConfigSchema());
}

QueryOptimizerConfiguration QueryOptimizerConfiguration::fromConfig(const InstantiatedConfig& config)
{
    return {.joinStrategy = config.get(JOIN_STRATEGY), .network = QueryOptimizerNetworkConfiguration::fromConfig(config)};
}

}
