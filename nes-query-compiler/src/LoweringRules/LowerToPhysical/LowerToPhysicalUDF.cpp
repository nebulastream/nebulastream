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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalUDF.hpp>

#include <memory>
#include <ranges>
#include <utility>
#include <vector>

#include <Functions/FunctionProvider.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UDFLogicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <UDFPhysicalOperator.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalUDF::apply(LogicalOperator logicalOperator)
{
    const auto udf = logicalOperator.getAs<UDFLogicalOperator>();
    if (udf->getExecution() != UdfExecution::InProcess)
    {
        throw NotImplemented("{} UDF execution", magic_enum::enum_name(udf->getExecution()));
    }

    const auto traitSet = logicalOperator.getTraitSet();
    const auto childTraitSet = udf->getChild().getTraitSet();
    const auto fieldMappingTrait = traitSet.get<FieldMappingTrait>();
    const auto outputSchema = logicalOperator.getOutputSchema();

    std::vector<UdfDescriptor> descriptors;
    std::vector<UDFPhysicalOperator::Argument> arguments;
    std::vector<UDFPhysicalOperator::Result> results;
    for (const auto& [name, function] : udf->getCalls())
    {
        const auto call = function.tryGetAs<UDFCallLogicalFunction>();
        PRECONDITION(call.has_value() && call.value()->getDescriptor().has_value(), "A UDF operator only holds resolved UDF calls");
        const auto& descriptor = *call.value()->getDescriptor();
        for (const auto& [argument, declaredType] : std::views::zip(function.getChildren(), descriptor.getArgTypes()))
        {
            arguments.push_back(
                {.function = QueryCompilation::FunctionProvider::lowerFunction(
                     argument, *childTraitSet.get<FieldMappingTrait>(), conf.getPythonUdfImportPaths()),
                 .declaredType = declaredType});
        }
        const auto resultField = outputSchema[name];
        INVARIANT(resultField.has_value(), "A UDF operator must output the result of its call {}", name);
        auto target = fieldMappingTrait->getMapping(resultField->unbound());
        PRECONDITION(target.has_value(), "UDF result was not in field mapping");
        results.push_back({.field = std::move(target).value(), .type = function.getDataType()});
        descriptors.push_back(descriptor);
    }

    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        UDFPhysicalOperator(std::move(arguments), std::move(results), descriptors),
        createPhysicalOutputSchema(childTraitSet),
        createPhysicalOutputSchema(traitSet),
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    return {.root = wrapper, .leaves = {wrapper}};
}

}
