/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactory.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <utility>

namespace NES {

GeneratableWindowOperator::GeneratableWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                                     GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id)
    : WindowLogicalOperatorNode(std::move(windowDefinition), id), generatableWindowAggregation(generatableWindowAggregation) {}

Windowing::AbstractWindowHandlerPtr GeneratableWindowOperator::createWindowHandler(SchemaPtr outputSchema) {
    return Windowing::WindowHandlerFactory::createAggregationWindowHandler(windowDefinition, outputSchema);
}

void GeneratableWindowOperator::generateSetupCode(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto tf = codegen->getTypeFactory();
    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto aggregation = windowDefinition->getWindowAggregation();

    auto isKeyed = windowDefinition->isKeyed();
    GeneratableDataTypePtr keyType;
    if (isKeyed) {
        auto logicalKeyType = windowDefinition->getOnKey()->getStamp();
        keyType = tf->createDataType(logicalKeyType);
    }
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto aggregationInputType = tf->createDataType(aggregation->getInputStamp());
    auto partialAggregateType = tf->createDataType(aggregation->getPartialAggregateStamp());
    auto finalAggregateType = tf->createDataType(aggregation->getFinalAggregateStamp());
    auto executableAggregation = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "aggregation");
    FunctionCallStatementPtr createAggregateCall;
    if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Sum) {
        createAggregateCall =
            codegen->call("Windowing::ExecutableSumAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
    }
    auto statement = VarDeclStatement(executableAggregation).assign(createAggregateCall);
    setupScope->addStatement(statement.copy());

    auto policy = windowDefinition->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = codegen->call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << policy->getPolicyType() << " not implemented");
    }

    auto action = windowDefinition->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Windowing::WindowAggregationTriggerAction) {
        auto createTriggerActionCall = codegen->call("Windowing::ExecutableCompleteAggregationTriggerAction<" + keyType->getCode()->code_ + ","
                      + aggregationInputType->getCode()->code_ + "," + partialAggregateType->getCode()->code_ + ","
                      + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else if (action->getActionType() == Windowing::SliceAggregationTriggerAction) {
        auto createTriggerActionCall = codegen->call("Windowing::ExecutableSliceAggregationTriggerAction<" + keyType->getCode()->code_ + ","
                                                         + aggregationInputType->getCode()->code_ + "," + partialAggregateType->getCode()->code_ + ","
                                                         + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << action->getActionType() << " not implemented");
    }

    auto windowHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");
}

}// namespace NES