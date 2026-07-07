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

#include <Configurations/SQLConfigValue.hpp>

#include <expected>
#include <utility>
#include <vector>

#include <Configurations/ConfigResolution.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>

namespace NES
{

std::expected<Schema<ConfigValue, Ordered>, InvalidSQLConfigSpecification>
resolveConfig(const Schema<SQLConfigValue, Ordered>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig)
{
    std::vector<std::pair<QualifiedIdentifier, ConfigLiteral>> passedLiterals;
    passedLiterals.reserve(passedConfig.size());
    for (const auto& passedValue : passedConfig)
    {
        passedLiterals.emplace_back(passedValue.getFullyQualifiedName(), passedValue.getValue());
    }
    return resolveConfig(passedLiterals, declaredConfig);
}

}
