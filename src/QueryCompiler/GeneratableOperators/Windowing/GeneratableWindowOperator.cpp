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
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
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

uint64_t GeneratableWindowOperator::generateSetupCode(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto tf = codegen->getTypeFactory();

    auto executionContextRef= VarRefStatement(context->code->varDeclarationExecutionContext);
    auto windowOperatorHandler = Windowing::WindowOperatorHandler::create(windowDefinition, outputSchema);
    auto windowOperatorIndex = context->registerOperatorHandler(windowOperatorHandler);


    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = codegen->call("getOperatorHandler<Windowing::WindowOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
                                       .assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = codegen->call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
        .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());



    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = codegen->call("getResultSchema");
    auto resultSchemaStatement = VarDeclStatement(resultSchemaDeclaration)
        .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());


    auto isKeyed = windowDefinition->isKeyed();
    GeneratableDataTypePtr keyType;
    if (isKeyed) {
        auto logicalKeyType = windowDefinition->getOnKey()->getStamp();
        keyType = tf->createDataType(logicalKeyType);
    }
    auto aggregation = windowDefinition->getWindowAggregation();
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
        createTriggerActionCall->addParameter(VarRef(resultSchemaDeclaration));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else if (action->getActionType() == Windowing::SliceAggregationTriggerAction) {
        auto createTriggerActionCall = codegen->call("Windowing::ExecutableSliceAggregationTriggerAction<" + keyType->getCode()->code_ + ","
                                                         + aggregationInputType->getCode()->code_ + "," + partialAggregateType->getCode()->code_ + ","
                                                         + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        createTriggerActionCall->addParameter(VarRef(resultSchemaDeclaration));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << action->getActionType() << " not implemented");
    }


    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto windowHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");
    auto createAggregationWindowHandlerCall = codegen->call("Windowing::AggregationWindowHandler<"
                                                     + keyType->getCode()->code_ + ","
                                                     + aggregationInputType->getCode()->code_ + ","
                                                     + partialAggregateType->getCode()->code_ + ","
                                                     + finalAggregateType->getCode()->code_ + ">::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(windowDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableAggregation));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    auto windowHandlerStatement = VarDeclStatement(windowHandler).assign(createAggregationWindowHandlerCall);
    setupScope->addStatement(windowHandlerStatement.copy());


    // windowOperatorHandler->setWindowHandler(windowHandler);
    auto setWindowHandlerCall = codegen->call("setWindowHandler");
    setWindowHandlerCall->addParameter(VarRef(windowHandler));
    auto setWindowHandlerStatement = VarRef(windowOperatorHandlerDeclaration).accessPtr(setWindowHandlerCall);
    setupScope->addStatement(setWindowHandlerStatement.copy());

    return windowOperatorIndex;
}

}// namespace NES