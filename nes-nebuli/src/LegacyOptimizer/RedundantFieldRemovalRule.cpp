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

#include <LegacyOptimizer/RedundantFieldRemovalRule.hpp>

#include <ranges>
#include <utility>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Common.hpp>

#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

void RedundantFieldRemovalRule::apply(LogicalPlan& queryPlan) ///NOLINT(readability-convert-member-functions-to-static)
{

    auto sink = queryPlan.getRootOperators()[0];
    //TODO: find all used fields in query
    auto fieldNames = sink.getOutputSchema().getFieldNames();
    if (sink.getChildren().size() > 0)
    {
        //avoid schema recreation on sink
        auto child = recursiveApply(sink.getChildren()[0], fieldNames);
        sink = sink.withChildren({child});
    }
    queryPlan = queryPlan.withRootOperators({sink});

}

LogicalOperator RedundantFieldRemovalRule::recursiveApply(LogicalOperator op, std::vector<std::string> fieldNames)
{
    if (op.getChildren().size() == 0)
    {
        return op;
    } //skip last op (source)}
    auto child = op.getChildren()[0];
    child = recursiveApply(child, fieldNames);
    auto oldInputSchema = child.getInputSchemas()[0];
    auto inputSchema = child.getOutputSchema();
    auto outputSchema = op.getOutputSchema();
    auto oldSchema = inputSchema;
    if (oldSchema.getFieldNames().size() > outputSchema.getFieldNames().size())
    {
        oldSchema = outputSchema;
    }

    std::vector schema = {Schema()};
    for (auto fieldName: fieldNames)
    {
        //if (outputSchema.contains(fieldName))
        //{
        auto field = oldSchema.getFieldByName(fieldName);
        schema[0].addField(field->name, field->dataType);
        //}
    }
    /*if (op.tryGet<SelectionLogicalOperator>())
    {
        auto selection = op.tryGet<SelectionLogicalOperator>();
        op = selection.value().withInferredSchema(inputSchema, schema[0]);
    }//TODO: map operator
    else*/
    op = op.withInferredSchema(schema);

    return op.withChildren({child});

}

}
