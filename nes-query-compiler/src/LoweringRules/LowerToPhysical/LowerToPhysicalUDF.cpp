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

#include <DataTypes/DataType.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UDFLogicalOperator.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <UDFBatchPhysicalOperator.hpp>
#include <UDFPhysicalOperator.hpp>
#include <UdfInterpreter.hpp>
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
    const auto logicalOutputSchema = logicalOperator.getOutputSchema();

    const auto inputSchema = createPhysicalOutputSchema(childTraitSet);
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    /// The batch holds the input records, the arguments and the results.
    std::vector<QualifiedUnboundField> batchFields(inputSchema.begin(), inputSchema.end());

    std::vector<UdfDescriptor> descriptors;
    std::vector<UDFBatchPhysicalOperator::Argument> arguments;
    std::vector<UDFPhysicalOperator::Result> results;
    for (const auto& [name, function] : udf->getCalls())
    {
        const auto call = function.tryGetAs<UDFCallLogicalFunction>();
        PRECONDITION(call.has_value() && call.value()->getDescriptor().has_value(), "A UDF operator only holds resolved UDF calls");
        const auto& descriptor = *call.value()->getDescriptor();
        for (const auto& [argument, declaredType] : std::views::zip(function.getChildren(), descriptor.getArgTypes()))
        {
            auto field = QualifiedIdentifier::parse(fmt::format("nes_udf_argument_{}", arguments.size()));
            batchFields.emplace_back(field, DataType{declaredType.type, DataType::NULLABLE::IS_NULLABLE});
            arguments.push_back(
                {.function = QueryCompilation::FunctionProvider::lowerFunction(
                     argument, *childTraitSet.get<FieldMappingTrait>(), conf.getPythonUdfImportPaths()),
                 .declaredType = declaredType,
                 .field = std::move(field)});
        }
        const auto resultField = logicalOutputSchema[name];
        INVARIANT(resultField.has_value(), "A UDF operator must output the result of its call {}", name);
        auto target = fieldMappingTrait->getMapping(resultField->unbound());
        PRECONDITION(target.has_value(), "UDF result was not in field mapping");
        batchFields.emplace_back(*target, DataType{function.getDataType().type, DataType::NULLABLE::IS_NULLABLE});
        results.push_back({.field = std::move(target).value(), .type = function.getDataType()});
        descriptors.push_back(descriptor);
    }

    const auto interpreter = [&]
    {
        switch (conf.inProcessUdfInterpreter)
        {
            case InProcessUdfInterpreter::SHARED:
                return UdfInterpreter::SHARED;
            case InProcessUdfInterpreter::SUBINTERPRETER:
                return UdfInterpreter::SUBINTERPRETER;
            case InProcessUdfInterpreter::DLMOPEN:
                return UdfInterpreter::DLMOPEN;
        }
        std::unreachable();
    }();
    auto physicalOperator = [&]() -> PhysicalOperator
    {
        if (conf.inProcessUdfCalls == InProcessUdfCalls::BATCH)
        {
            return UDFBatchPhysicalOperator(
                std::move(arguments),
                results
                    | std::views::transform([](const auto& result)
                                            { return UDFBatchPhysicalOperator::Result{.field = result.field, .type = result.type}; })
                    | std::ranges::to<std::vector>(),
                descriptors,
                interpreter,
                std::dynamic_pointer_cast<ArrowBufferRef>(LowerSchemaProvider::lowerSchemaToArrow(
                    conf.operatorBufferSize, batchFields | std::ranges::to<Schema<QualifiedUnboundField, Ordered>>())),
                outputSchema | std::views::transform([](const auto& field) { return field.getFullyQualifiedName(); })
                    | std::ranges::to<std::vector>(),
                conf.inProcessUdfFusion);
        }
        return UDFPhysicalOperator(
            arguments
                | std::views::transform(
                    [](const auto& argument)
                    { return UDFPhysicalOperator::Argument{.function = argument.function, .declaredType = argument.declaredType}; })
                | std::ranges::to<std::vector>(),
            std::move(results),
            descriptors,
            interpreter,
            conf.inProcessUdfFusion);
    }();

    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        std::move(physicalOperator),
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);
    return {.root = wrapper, .leaves = {wrapper}};
}

}
