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
#include <LegacyOptimizer/InlineSourceBindingPhase.hpp>

#include <utility>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
void InlineSourceBindingPhase::apply(LogicalPlan& queryPlan) const
{
    for (const auto& inlineSource : getOperatorByType<InlineSourceLogicalOperator>(queryPlan))
    {
        auto type = inlineSource->getSourceType();
        auto schema = inlineSource->getSchema();
        auto parserConfig = inlineSource->getParserConfig();
        auto sourceConfig = inlineSource->getSourceConfig();

        auto descriptorOpt = sourceCatalog->getInlineSource(type, schema, parserConfig, sourceConfig);

        if (!descriptorOpt.has_value())
        {
            throw InvalidConfigParameter("Could not create an inline source descriptor because of invalid config parameters");
        }
        const auto& descriptor = descriptorOpt.value();
        const SourceDescriptorLogicalOperator sourceDescriptorLogicalOperator{descriptor};

        auto result = replaceOperator(queryPlan, inlineSource.getId(), sourceDescriptorLogicalOperator);

        if (!result.has_value())
        {
            throw InvalidConfigParameter("Could not create an inline source descriptor operator because of invalid config parameters");
        }

        queryPlan = std::move(*result);
    }
}

}
