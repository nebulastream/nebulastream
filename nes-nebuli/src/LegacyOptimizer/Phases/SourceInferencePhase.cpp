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

#include <LegacyOptimizer/Phases/SourceInferencePhase.hpp>

#include <algorithm>
#include <ranges>
#include <utility>
#include <memory>

#include <DataTypes/Schema.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Pointers.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

LogicalPlan SourceInferencePhase::apply(const LogicalPlan& inputPlan, SharedPtr<const SourceCatalog> sourceCatalog)
{
    LogicalPlan plan = inputPlan;
    auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(inputPlan);

    PRECONDITION(not sourceOperators.empty(), "Query plan did not contain sources during type inference.");

    for (auto& source : sourceOperators)
    {
        /// if the source descriptor has no schema set and is only a logical source we replace it with the correct
        /// source descriptor from the catalog.
        auto schema = Schema();
        auto logicalSourceOpt = sourceCatalog->getLogicalSource(source.getLogicalSourceName());
        if (not logicalSourceOpt.has_value())
        {
            throw UnknownSourceName("Logical source not registered. Source Name: {}", source.getLogicalSourceName());
        }
        const auto& logicalSource = logicalSourceOpt.value();
        schema.appendFieldsFromOtherSchema(*logicalSource.getSchema());
        schema.memoryLayoutType = logicalSource.getSchema()->memoryLayoutType;
        auto qualifierName = source.getLogicalSourceName() + Schema::ATTRIBUTE_NAME_SEPARATOR;
        /// perform attribute name resolution
        std::ranges::for_each(
            schema.getFields()
                | std::views::filter([&qualifierName](const auto& field) { return not field.name.starts_with(qualifierName); }),
            [&qualifierName, &schema](auto& field)
            {
                if (not schema.renameField(field.name, qualifierName + field.name))
                {
                    throw CannotInferSchema("Could not rename non-existing field: {}", field.name);
                }
            });
        plan = *replaceOperator(plan, source.id, source.withSchema(schema));
    }
    return plan;
}

}
