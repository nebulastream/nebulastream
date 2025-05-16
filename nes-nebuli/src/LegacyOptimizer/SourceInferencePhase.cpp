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

#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <LegacyOptimizer/SourceInferencePhase.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

void SourceInferencePhase::apply(LogicalPlan& queryPlan) const
{
    auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(queryPlan);

    PRECONDITION(not sourceOperators.empty(), "Query plan did not contain sources during type inference.");

    for (auto& source : sourceOperators)
    {
        /// if the source descriptor has no schema set and is only a logical source we replace it with the correct
        /// source descriptor form the catalog.
        auto schema = Schema();
        if (!sourceCatalog->containsLogicalSource(source.getLogicalSourceName()))
        {
            throw LogicalSourceNotFoundInQueryDescription("Logical source not registered. Source Name: {}", source.getLogicalSourceName());
        }
        auto originalSchema = sourceCatalog->getSchemaForLogicalSource(source.getLogicalSourceName());
        schema = schema.copyFields(originalSchema);
        schema.setLayoutType(originalSchema.getLayoutType());
        auto qualifierName = source.getLogicalSourceName() + Schema::ATTRIBUTE_NAME_SEPARATOR;
        /// perform attribute name resolution
        for (auto& field : schema)
        {
            if (!field.getName().starts_with(qualifierName))
            {
                field.setName(qualifierName + field.getName());
            }
        }
        auto result = replaceOperator(queryPlan, source, source.withSchema(schema));
        INVARIANT(result.has_value(), "replaceOperator failed");
        queryPlan = std::move(*result);
    }
}
}
