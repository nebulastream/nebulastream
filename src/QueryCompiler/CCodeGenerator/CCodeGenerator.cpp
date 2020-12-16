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

#include <API/UserAPIExpression.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/ClassDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/NamespaceDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Runtime/SharedPointerGen.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ContinueStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipelineStage.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableCountAggregation.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowActions/BaseJoinActionDescriptor.hpp>
namespace NES {

CCodeGenerator::CCodeGenerator() : CodeGenerator(), compiler(Compiler::create()) {}

StructDeclaration CCodeGenerator::getStructDeclarationFromSchema(std::string structName, SchemaPtr schema) {
    /* struct definition for tuples */
    StructDeclaration structDeclarationTuple = StructDeclaration::create(structName, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();

    NES_DEBUG("Converting Schema: " << schema->toString());
    NES_DEBUG("Define Struct : " << structName);

    for (uint64_t i = 0; i < schema->getSize(); ++i) {
        structDeclarationTuple.addField(VariableDeclaration::create(schema->get(i)->getDataType(), schema->get(i)->name));
        NES_DEBUG("Field " << i << ": " << schema->get(i)->getDataType()->toString() << " " << schema->get(i)->name);
    }
    return structDeclarationTuple;
}

StructDeclaration CCodeGenerator::getStructDeclarationFromWindow(std::string structName) {
    StructDeclaration structDeclarationTuple = StructDeclaration::create(structName, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();

    SchemaPtr schema = Schema::create()
                           ->addField(createField("start", UINT64))
                           ->addField(createField("end", UINT64))
                           ->addField(createField("key", UINT64))
                           ->addField("value", UINT64);

    for (uint64_t i = 0; i < schema->getSize(); ++i) {
        structDeclarationTuple.addField(VariableDeclaration::create(schema->get(i)->getDataType(), schema->get(i)->name));
        NES_DEBUG("Field " << i << ": " << schema->get(i)->getDataType()->toString() << " " << schema->get(i)->name);
    }
    return structDeclarationTuple;
}

const VariableDeclarationPtr getVariableDeclarationForField(const StructDeclaration& structDeclaration,
                                                            const AttributeFieldPtr field) {
    if (structDeclaration.getField(field->name))
        return std::make_shared<VariableDeclaration>(structDeclaration.getVariableDeclaration(field->name));
    else {
        return VariableDeclarationPtr();
    }
}

const std::string toString(void*, DataTypePtr) {
    //     if(type->)
    return "";
}

CodeGeneratorPtr CCodeGenerator::create() { return std::make_shared<CCodeGenerator>(); }

bool CCodeGenerator::generateCodeForScan(SchemaPtr inputSchema, SchemaPtr outputSchema, PipelineContextPtr context) {

    context->inputSchema = outputSchema->copy();
    auto code = context->code;
    code->structDeclaratonInputTuple = getStructDeclarationFromSchema("InputTuple", inputSchema);

    /** === set the result tuple depending on the input tuple===*/
    context->resultSchema = outputSchema;
    code->structDeclarationResultTuple = getStructDeclarationFromSchema("ResultTuple", outputSchema);
    auto tf = getTypeFactory();
    /* === declarations === */
    auto tupleBufferType = tf->createAnonymusDataType("NES::NodeEngine::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("NodeEngine::Execution::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("NodeEngine::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer =
        VariableDeclaration::create(tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    VariableDeclaration varDeclarationWorkerContext =
        VariableDeclaration::create(tf->createReference(workerContextType), "workerContext");

    auto varDeclarationResultBuffer = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");

    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationWorkerContext = varDeclarationWorkerContext;
    code->varDeclarationResultBuffer = varDeclarationResultBuffer;

    /* declaration of record index; */
    code->varDeclarationRecordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "recordIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());
    /* int32_t ret = 0; */
    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt32()), "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());

    code->varDeclarationInputTuples = VariableDeclaration::create(
        tf->createPointer(tf->createUserDefinedType(code->structDeclaratonInputTuple)), "inputTuples");

    code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));

    /*  tuples = (InputTuple *)input_buffer.getBuffer()*/
    code->variableInitStmts.push_back(
        VarDeclStatement(varDeclarationResultBuffer).assign(allocateTupleBuffer(varDeclarationPipelineExecutionContext)).copy());

    code->variableInitStmts.push_back(
        VarDeclStatement(code->varDeclarationInputTuples)
            .assign(getTypedBuffer(code->varDeclarationInputBuffer, code->structDeclaratonInputTuple))
            .copy());

    /* for (uint64_t recordIndex = 0; recordIndex < tuple_buffer_1->num_tuples; ++id) */
    // input_buffer.getNumberOfTuples()
    auto numberOfRecords = VarRef(varDeclarationInputBuffer).accessRef(context->code->tupleBufferGetNumberOfTupleCall);
    code->forLoopStmt = std::make_shared<FOR>(code->varDeclarationRecordIndex,
                                              (VarRef(code->varDeclarationRecordIndex) < (numberOfRecords)).copy(),
                                              (++VarRef(code->varDeclarationRecordIndex)).copy());

    code->currentCodeInsertionPoint = code->forLoopStmt->getCompoundStatement();

    code->returnStmt = ReturnStatement::create(VarRefStatement(*code->varDeclarationReturnValue).createCopy());
    return true;
}

/**
 * generates code for predicates
 * @param pred - defined predicate for the query
 * @param context - includes the context of the used fields
 * @param out - sending some other information if wanted
 * @return modified query-code
 */
bool CCodeGenerator::generateCodeForFilter(PredicatePtr pred, PipelineContextPtr context) {

    // create predicate expression from filter predicate
    auto predicateExpression = pred->generateCode(context->code);
    // create if statement
    auto ifStatement = IF(*predicateExpression);
    // update current compound_stmt
    context->code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());
    context->code->currentCodeInsertionPoint = ifStatement.getCompoundStatement();

    return true;
}

/**
 * generates code for a mapper with an defined AttributeField and a PredicatePtr
 * @param field - existing or new created field that includes the mapped function
 * @param pred - mapping function as a predicate tree for easy single lined functions.
 * @param context - includes the context of the used fields
 * @param out - sending some other information if wanted
 * @return modified query-code
 */
bool CCodeGenerator::generateCodeForMap(AttributeFieldPtr field, UserAPIExpressionPtr pred, PipelineContextPtr context) {

    auto code = context->code;
    auto tf = getTypeFactory();
    auto varDeclarationResultTuples = VariableDeclaration::create(
        tf->createPointer(tf->createUserDefinedType(code->structDeclarationResultTuple)), "resultTuples");

    auto declaredMapVar = getVariableDeclarationForField(code->structDeclarationResultTuple, field);
    if (!declaredMapVar) {
        context->resultSchema->addField(field);
        context->code->structDeclarationResultTuple.addField(VariableDeclaration::create(field->dataType, field->name));
        declaredMapVar = getVariableDeclarationForField(code->structDeclarationResultTuple, field);
    }
    code->override_fields.push_back(declaredMapVar);
    auto varMapI = code->structDeclarationResultTuple.getVariableDeclaration(field->name);
    auto callVar =
        VarRef(varDeclarationResultTuples)[VarRef(code->varDeclarationNumberOfResultTuples)].accessRef(VarRef(varMapI));
    auto expr = pred->generateCode(code);
    auto assignedMap = (callVar).assign(*expr);
    code->currentCodeInsertionPoint->addStatement(assignedMap.copy());

    return true;
}

bool CCodeGenerator::generateCodeForEmit(SchemaPtr sinkSchema, PipelineContextPtr context) {
    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for Sink.");
    auto code = context->code;
    // set result schema to context
    context->resultSchema = sinkSchema;
    // generate result tuple struct
    auto structDeclarationResultTuple = getStructDeclarationFromSchema("ResultTuple", sinkSchema);
    // add type declaration for the result tuple
    code->typeDeclarations.push_back(structDeclarationResultTuple);

    auto varDeclResultTuple =
        VariableDeclaration::create(tf->createPointer(tf->createUserDefinedType(structDeclarationResultTuple)), "resultTuples");

    /* ResultTuple* resultTuples = (ResultTuple*)resultTupleBuffer.getBuffer();*/
    code->variableInitStmts.push_back(VarDeclStatement(varDeclResultTuple)
                                          .assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple))
                                          .copy());

    /**
     * @brief TODO the following code is black magic it need to be refactored in a larger issue.
     */
    for (uint64_t i = 0; i < context->resultSchema->getSize(); ++i) {
        auto resultField = context->resultSchema->get(i);
        auto variableDeclaration = getVariableDeclarationForField(structDeclarationResultTuple, resultField);
        if (!variableDeclaration) {
            NES_ERROR("CodeGenerator: Could not extract field " << resultField->toString() << " from struct "
                                                                << structDeclarationResultTuple.getTypeName());
            NES_DEBUG("CodeGenerator: W>");
        }
        auto varDeclarationInput = getVariableDeclarationForField(code->structDeclarationResultTuple, resultField);
        if (varDeclarationInput) {
            bool override = false;
            for (uint64_t j = 0; j < code->override_fields.size(); j++) {
                if (code->override_fields.at(j)->getIdentifierName().compare(varDeclarationInput->getIdentifierName()) == 0) {
                    override = true;
                    break;
                }
            }
            if (!override) {
                auto varDeclarationField = getVariableDeclarationForField(structDeclarationResultTuple, resultField);
                if (!varDeclarationField) {
                    NES_ERROR("Could not extract field " << resultField->toString() << " from struct "
                                                         << structDeclarationResultTuple.getTypeName());
                    NES_DEBUG("W>");
                }
                //context->code->variableDeclarations.push_back(*varDeclarationField);
                AssignmentStatment as = {varDeclResultTuple,
                                         *(varDeclarationField),
                                         code->varDeclarationNumberOfResultTuples,
                                         code->varDeclarationInputTuples,
                                         *(varDeclarationField),
                                         *(code->varDeclarationRecordIndex)};
                auto copyFieldStatement = varDeclarationField->getDataType()->getStmtCopyAssignment(as);
                code->currentCodeInsertionPoint->addStatement(copyFieldStatement);
            }
        }
    }

    // increment number of tuples in buffer -> ++numberOfResultTuples;
    code->currentCodeInsertionPoint->addStatement((++VarRef(code->varDeclarationNumberOfResultTuples)).copy());

    // generate logic to check if tuple buffer is already full. If so we emit the current one and pass it to the runtime.
    generateTupleBufferSpaceCheck(context, varDeclResultTuple, structDeclarationResultTuple);

    // Generate final logic to emit the last buffer to the runtime
    // 1. set the number of tuples to the buffer
    code->cleanupStmts.push_back(
        setNumberOfTuples(code->varDeclarationResultBuffer, code->varDeclarationNumberOfResultTuples).copy());

    // 2. copy watermark
    code->cleanupStmts.push_back(setWatermark(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 3. copy origin id
    code->cleanupStmts.push_back(setOriginId(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 4. emit the buffer to the runtime.
    code->cleanupStmts.push_back(
        emitTupleBuffer(code->varDeclarationExecutionContext, code->varDeclarationResultBuffer, code->varDeclarationWorkerContext)
            .copy());

    return true;
}

bool CCodeGenerator::generateCodeForWatermarkAssigner(Windowing::WatermarkStrategyPtr watermarkStrategy,
                                                      PipelineContextPtr context) {
    if (watermarkStrategy->getType() == Windowing::WatermarkStrategy::EventTimeWatermark) {
        Windowing::EventTimeWatermarkStrategyPtr eventTimeWatermarkStrategy =
            watermarkStrategy->as<Windowing::EventTimeWatermarkStrategy>();
        auto watermarkFieldName = eventTimeWatermarkStrategy->getField()->getFieldName();
        NES_ASSERT(context->getInputSchema()->has(watermarkFieldName),
                   "CCOdeGenerator: watermark assigner could not get field \"" << watermarkFieldName << "\" from struct");

        AttributeFieldPtr attribute = AttributeField::create(watermarkFieldName, DataTypeFactory::createInt64());

        auto tf = getTypeFactory();

        // initiate maxWatermark variable
        // auto maxWatermark = 0;
        auto maxWatermarkVariableDeclaration =
            VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), "maxWatermark");
        auto maxWatermarkInitStatement =
            VarDeclStatement(maxWatermarkVariableDeclaration)
                .assign(Constant(
                    tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))));
        context->code->variableInitStmts.push_back(std::make_shared<BinaryOperatorStatement>(maxWatermarkInitStatement));

        // get the value for current watermark
        // auto currentWatermark = record[index].ts;
        auto currentWatermarkVariableDeclaration =
            VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), "currentWatermark");
        auto tsVariableDeclaration = context->code->structDeclaratonInputTuple.getVariableDeclaration(attribute->name);
        auto calculateMaxTupleStatement =
            VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                VarRef(tsVariableDeclaration))
                * ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(
                    BasicType::UINT64, std::to_string(eventTimeWatermarkStrategy->getMultiplier()))))
            - Constant(tf->createValueType(DataTypeFactory::createBasicValue(
                DataTypeFactory::createUInt64(), std::to_string(eventTimeWatermarkStrategy->getAllowedLateness()))));
        auto currentWatermarkStatement = VarDeclStatement(currentWatermarkVariableDeclaration).assign(calculateMaxTupleStatement);
        context->code->currentCodeInsertionPoint->addStatement(
            std::make_shared<BinaryOperatorStatement>(currentWatermarkStatement));

        // Check and update max watermark if current watermark is greater than maximum watermark
        // if (currentWatermark > maxWatermark) {
        //     maxWatermark = currentWatermark;
        // };
        auto updateMaxWatermarkStatement =
            VarRef(maxWatermarkVariableDeclaration).assign(VarRef(currentWatermarkVariableDeclaration));
        auto ifStatement = IF(VarRef(currentWatermarkVariableDeclaration) > VarRef(maxWatermarkVariableDeclaration),
                              updateMaxWatermarkStatement);
        context->code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());

        // set the watermark of input buffer based on maximum watermark
        // inputTupleBuffer.setWatermark(maxWatermark);
        auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
        setWatermarkFunctionCall.addParameter(VarRef(maxWatermarkVariableDeclaration));
        auto setWatermarkStatement = VarRef(context->code->varDeclarationInputBuffer).accessRef(setWatermarkFunctionCall);
        context->code->cleanupStmts.push_back(setWatermarkStatement.createCopy());
    } else if (watermarkStrategy->getType() == Windowing::WatermarkStrategy::IngestionTimeWatermark) {
        // get the watermark from attribute field
        // auto watermark_ts = NES::Windowing::getTsFromClock()
        auto tf = getTypeFactory();
        auto watermarkTsVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "watermark_ts");
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(watermarkTsVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));

        // set the watermark
        // inputTupleBuffer.setWatermark(watermark_ts);
        auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
        setWatermarkFunctionCall.addParameter(VarRef(watermarkTsVariableDeclaration));
        auto setWatermarkStatement = VarRef(context->code->varDeclarationInputBuffer).accessRef(setWatermarkFunctionCall);

        context->code->currentCodeInsertionPoint->addStatement(setWatermarkStatement.createCopy());
    } else {
        NES_ERROR("CCodeGenerator: cannot generate code for watermark strategy " << watermarkStrategy);
    }

    return true;
}

void CCodeGenerator::generateCodeForWatermarkUpdater(PipelineContextPtr context, VariableDeclaration handler) {
    // updateMaxTs(maxWaterMark, inputBuffer.getOriginId())
    auto updateAllWatermarkTsFunctionCall = FunctionCallStatement("updateMaxTs");
    updateAllWatermarkTsFunctionCall.addParameter(getWatermark(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(getOriginId(context->code->varDeclarationInputBuffer));
    auto updateAllWatermarkTsFunctionCallStatement = VarRef(handler).accessPtr(updateAllWatermarkTsFunctionCall);

    context->code->cleanupStmts.push_back(updateAllWatermarkTsFunctionCallStatement.createCopy());
}

void CCodeGenerator::generateTupleBufferSpaceCheck(PipelineContextPtr context, VariableDeclaration varDeclResultTuple,
                                                   StructDeclaration structDeclarationResultTuple) {
    NES_DEBUG("CCodeGenerator: Generate code for tuple buffer check");

    auto code = context->code;
    auto tf = getTypeFactory();

    // calculate of the maximal number of tuples per buffer -> (buffer size / tuple size) - 1
    // int64_t maxTuple = (resultTupleBuffer.getBufferSize() / 39) - 1;
    // 1. get the size of one result tuple
    auto resultTupleSize = context->getResultSchema()->getSchemaSizeInBytes();
    // 2. initialize max tuple
    auto maxTupleDeclaration = VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt64()), "maxTuple");
    // 3. create calculation statement
    auto calculateMaxTupleStatement = getBufferSize(code->varDeclarationResultBuffer)
        / Constant(tf->createValueType(
            DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(resultTupleSize))));
    auto calculateMaxTupleAssignment = VarDeclStatement(maxTupleDeclaration).assign(calculateMaxTupleStatement);
    // 4. add statement
    code->currentCodeInsertionPoint->addStatement(calculateMaxTupleAssignment.copy());

    // Check if maxTuple is reached. -> maxTuple <= numberOfResultTuples
    auto ifStatement = IF((VarRef(code->varDeclarationNumberOfResultTuples)) >= VarRef(maxTupleDeclaration));
    // add if statement to current code block
    code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());
    // add tuple emit logic to then statement, which is executed if the condition is met.
    // In this case we 1. emit the buffer and 2. allocate a new buffer.
    auto thenStatement = ifStatement.getCompoundStatement();
    // 1.1 set the number of tuples to the output buffer -> resultTupleBuffer.setNumberOfTuples(numberOfResultTuples);
    thenStatement->addStatement(
        setNumberOfTuples(code->varDeclarationResultBuffer, code->varDeclarationNumberOfResultTuples).copy());

    // 1.1 set the origin id to the output buffer -> resultTupleBuffer.setOriginId(numberOfResultTuples);

    thenStatement->addStatement(setOriginId(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());
    // 1.1 set the watermar to the output buffer -> resultTupleBuffer.setWatermark(numberOfResultTuples);
    thenStatement->addStatement(setWatermark(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 1.2 emit the output buffers to the runtime -> pipelineExecutionContext.emitBuffer(resultTupleBuffer);
    thenStatement->addStatement(
        emitTupleBuffer(code->varDeclarationExecutionContext, code->varDeclarationResultBuffer, code->varDeclarationWorkerContext)
            .copy());
    // 2.1 reset the numberOfResultTuples to 0 -> numberOfResultTuples = 0;
    thenStatement->addStatement(VarRef(code->varDeclarationNumberOfResultTuples)
                                    .assign(Constant(tf->createValueType(
                                        DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))))
                                    .copy());
    // 2.2 allocate a new buffer -> resultTupleBuffer = pipelineExecutionContext.allocateTupleBuffer();
    thenStatement->addStatement(
        VarRef(code->varDeclarationResultBuffer).assign(allocateTupleBuffer(code->varDeclarationExecutionContext)).copy());
    // 2.2 get typed result buffer from resultTupleBuffer -> resultTuples = (ResultTuple*)resultTupleBuffer.getBuffer();
    thenStatement->addStatement(
        VarRef(varDeclResultTuple).assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple)).copy());
}

/**
 * Code Generation for the window operator
 * @param window windowdefinition
 * @param context pipeline context
 * @param out
 * @return
 */
bool CCodeGenerator::generateCodeForCompleteWindow(Windowing::LogicalWindowDefinitionPtr window,
                                                   GeneratableWindowAggregationPtr generatableWindowAggregation,
                                                   PipelineContextPtr context, uint64_t windowOperatorIndex) {

    auto tf = getTypeFactory();
    auto windowOperatorHandlerDeclaration = getWindowOperatorHandler(context, context->code->varDeclarationExecutionContext, windowOperatorIndex);
    auto windowHandlerVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");
    auto keyStamp = window->isKeyed() ? window->getOnKey()->getStamp():  window->getWindowAggregation()->on()->getStamp();
    auto getWindowHandlerStatement = getAggregationWindowHandler(
        windowOperatorHandlerDeclaration, keyStamp,
        window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
        window->getWindowAggregation()->getFinalAggregateStamp());
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());


    auto constStatement =
        ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), "0")));

    if (context->pipelineName != "SlicingWindowType") {
        context->pipelineName = "CompleteWindowType";
    }

    auto debugDecl = VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), context->pipelineName);
    auto debState = VarDeclStatement(debugDecl).assign(
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))));
    context->code->variableInitStmts.push_back(std::make_shared<BinaryOperatorStatement>(debState));

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");


    NES_ASSERT(!window->getWindowAggregation()->getInputStamp()->isUndefined(), "window input type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getPartialAggregateStamp()->isUndefined(), "window partial type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getFinalAggregateStamp()->isUndefined(), "window final type is undefined");
/*
    if (window->isKeyed()) {
        auto getWindowHandlerStatement = getAggregationWindowHandler(
            context->code->varDeclarationExecutionContext, window->getOnKey()->getStamp(),
            window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
            window->getWindowAggregation()->getFinalAggregateStamp());
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());
    } else {
       auto getWindowHandlerStatement = getAggregationWindowHandler(
            context->code->varDeclarationExecutionContext, window->getWindowAggregation()->on()->getStamp(),
            window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
            window->getWindowAggregation()->getFinalAggregateStamp());
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());
    }
    */

    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());

    // get allowed lateness
    //    auto allowedLateness = windowManager->getAllowedLateness();
    auto latenessHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "allowedLateness");
    auto getAllowedLatenessStateVariable = FunctionCallStatement("getAllowedLateness");
    auto allowedLatenessHandlerVariableStatement = VarRef(windowManagerVarDeclaration).accessPtr(getAllowedLatenessStateVariable);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(latenessHandlerVariableDeclaration).assign(allowedLatenessHandlerVariableStatement).copy());

    // Read key value from record
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt64()), "key");
    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration =
            context->code->structDeclaratonInputTuple.getVariableDeclaration(window->getOnKey()->getFieldName());
        auto keyVariableAttributeStatement =
            VarDeclStatement(keyVariableDeclaration)
                .assign(
                    VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                        VarRef(keyVariableAttributeDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(
            std::make_shared<BinaryOperatorStatement>(keyVariableAttributeStatement));
    } else {
        auto defaultKeyAssignment = VarDeclStatement(keyVariableDeclaration)
                                        .assign(ConstantExpressionStatement(tf->createValueType(
                                            DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(defaultKeyAssignment));
    }

    // get key handle for current key
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");

    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(keyHandlerVariableStatement));
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    // set the default value for window state
    if (std::dynamic_pointer_cast<Windowing::MinAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
            tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "INT64_MAX"))));
    } else if (std::dynamic_pointer_cast<Windowing::MaxAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
            tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "INT64_MIN"))));
    } else if (std::dynamic_pointer_cast<Windowing::SumAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
            tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
    } else if (std::dynamic_pointer_cast<Windowing::CountAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
            tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
    } else {
        NES_FATAL_ERROR("Window Handler: could not cast aggregation type");
    }

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));

    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(windowStateVariableStatement));

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (window->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));
    } else {
        auto tsVariableDeclaration = context->code->structDeclaratonInputTuple.getVariableDeclaration(
            window->getWindowType()->getTimeCharacteristic()->getField()->name);
        //TODO: get unit
        /**
         * calculateUnitMultiplier => cal to ms
         */
        auto multiplier = window->getWindowType()->getTimeCharacteristic()->getTimeUnit().getMultiplier();
        //In this case we need to multiply the ts with the multiplier to get ms
        auto tsVariableDeclarationStatement =
            VarDeclStatement(currentTimeVariableDeclaration)
                .assign(
                    VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                        VarRef(tsVariableDeclaration))
                    * ConstantExpressionStatement(
                        tf->createValueType(DataTypeFactory::createBasicValue(BasicType::UINT64, std::to_string(multiplier)))));
        context->code->currentCodeInsertionPoint->addStatement(
            std::make_shared<BinaryOperatorStatement>(tsVariableDeclarationStatement));
    }

    //within the loop
    //get min watermark
    auto minWatermarkVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "minWatermark");
    auto getMinWatermarkStateVariable = FunctionCallStatement("getMinWatermark");
    auto minWatermarkHandlerVariableStatement =
        VarDeclStatement(minWatermarkVariableDeclaration)
            .assign(VarRef(windowHandlerVariableDeclration).accessPtr(getMinWatermarkStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(minWatermarkHandlerVariableStatement));

    //        if (ts < (minWatermark - allowedLatness)
    //{continue;}
    auto ifStatementAllowedLateness = IF(VarRef(currentTimeVariableDeclaration) < VarRef(minWatermarkVariableDeclaration)
                                                 - VarRef(latenessHandlerVariableDeclaration),
                                         Continue());
    context->code->currentCodeInsertionPoint->addStatement(ifStatementAllowedLateness.createCopy());

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    //only in debug mode add the key for debugging
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(currentSliceIndexVariableStatement));

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration).assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    generatableWindowAggregation->compileLiftCombine(
        context->code->currentCodeInsertionPoint, partialRef, context->code->structDeclaratonInputTuple,
        VarRef(context->code->varDeclarationInputTuples)[VarRefStatement(VarRef(*(context->code->varDeclarationRecordIndex)))]);

    // get the slice metadata aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getSliceMetadata = FunctionCallStatement("getSliceMetadata");
    auto getSliceMetadataCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceMetadata);
    VariableDeclaration sliceMetadataDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "sliceMetaData");
    auto sliceAssigment = VarDeclStatement(sliceMetadataDeclaration).assign(getSliceMetadataCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(sliceAssigment));

    auto getSliceCall = FunctionCallStatement("incrementRecordsPerSlice");
    auto updateSliceStatement = VarRef(sliceMetadataDeclaration)[current_slice_ref].accessRef(getSliceCall);
    context->code->currentCodeInsertionPoint->addStatement(updateSliceStatement.createCopy());

    // windowHandler->trigger();
    switch (window->getTriggerPolicy()->getPolicyType()) {
        case Windowing::triggerOnRecord: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->currentCodeInsertionPoint->addStatement(call);
            break;
        }
        case Windowing::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdater(context, windowHandlerVariableDeclration);
    return true;
}

bool CCodeGenerator::generateCodeForSlicingWindow(Windowing::LogicalWindowDefinitionPtr window,
                                                  GeneratableWindowAggregationPtr generatableWindowAggregation,
                                                  PipelineContextPtr context,
                                                  uint64_t windowOperatorId) {
    NES_DEBUG("CCodeGenerator::generateCodeForSlicingWindow with " << window << " pipeline " << context);
    //NOTE: the distinction currently only happens in the trigger
    context->pipelineName = "SlicingWindowType";
    return generateCodeForCompleteWindow(window, generatableWindowAggregation, context, windowOperatorId);
}

uint64_t CCodeGenerator::generateJoinSetup(Join::LogicalJoinDefinitionPtr join, PipelineContextPtr context) {
    auto tf = getTypeFactory();

    auto executionContextRef= VarRefStatement(context->code->varDeclarationExecutionContext);
    auto joinOperatorHandler = Join::JoinOperatorHandler::create(join, context->getResultSchema());
    auto joinOperatorHandlerIndex = context->registerOperatorHandler(joinOperatorHandler);


    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::JoinOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorHandlerIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
        .assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto joinDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinDefinition");
    auto getWindowDefinitionCall = call("getJoinDefinition");
    auto windowDefinitionStatement = VarDeclStatement(joinDefDeclaration)
        .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());


    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement = VarDeclStatement(resultSchemaDeclaration)
        .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyType = tf->createDataType(join->getJoinKey()->getStamp());

    auto policy = join->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << policy->getPolicyType() << " not implemented");
    }

    auto action = join->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Join::JoinActionType::LazyNestedLoopJoin) {
        auto createTriggerActionCall = call("Join::ExecutableNestedLoopJoinTriggerAction<" + keyType->getCode()->code_ +">::create");
        createTriggerActionCall->addParameter(VarRef(joinDefDeclaration));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << action->getActionType() << " not implemented");
    }


    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto joinHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto createAggregationWindowHandlerCall = call("Join::JoinHandler<"
                                                       + keyType->getCode()->code_ + ">::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(joinDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    auto windowHandlerStatement = VarDeclStatement(joinHandler).assign(createAggregationWindowHandlerCall);
    setupScope->addStatement(windowHandlerStatement.copy());


    // windowOperatorHandler->setWindowHandler(windowHandler);
    auto setWindowHandlerCall = call("setJoinHandler");
    setWindowHandlerCall->addParameter(VarRef(joinHandler));
    auto setWindowHandlerStatement = VarRef(windowOperatorHandlerDeclaration).accessPtr(setWindowHandlerCall);
    setupScope->addStatement(setWindowHandlerStatement.copy());

    // setup window handler
    auto getSharedFromThis = call("shared_from_this");
    auto setUpWindowHandlerCall = call("setup");
    setUpWindowHandlerCall->addParameter(VarRef(context->code->varDeclarationExecutionContext).accessRef(getSharedFromThis));

    auto setupWindowHandlerStatement = VarRef(joinHandler).accessPtr(setUpWindowHandlerCall);
    setupScope->addStatement(setupWindowHandlerStatement.copy());
    return joinOperatorHandlerIndex;
}

bool CCodeGenerator::generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef, PipelineContextPtr context, uint64_t operatorHandlerIndex) {
    //    std::cout << joinDef << context << std::endl;
    auto tf = getTypeFactory();
    NES_ASSERT(joinDef, "invalid join definition");
    NES_DEBUG("CCodeGenerator: Generate code for join" << joinDef);
    auto code = context->code;

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");
    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");

    NES_ASSERT(!joinDef->getJoinKey()->getStamp()->isUndefined(), "left join key is undefined");

    auto windowOperatorHandlerDeclaration = getJoinOperatorHandler(context, context->code->varDeclarationExecutionContext,
                                                                     operatorHandlerIndex);
    auto windowJoinVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto keyStamp = joinDef->getJoinKey()->getStamp();
    auto getJoinHandlerStatement = getJoinWindowHandler(windowOperatorHandlerDeclaration, keyStamp);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowJoinVariableDeclration).assign(getJoinHandlerStatement).copy());


    //-------------------------

    auto getWindowManagerStatement = getWindowManager(windowJoinVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    if (context->isLeftSide) {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side left");
        auto getWindowStateStatement = getLeftJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    } else {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side right");
        auto getWindowStateStatement = getRightJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    }

    /**
    * within the loop
    */
    // Read key value from record
    // int64_t key = windowTuples[recordIndex].key;
    //TODO we have to change this depending on the pipeline
    auto keyVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(joinDef->getJoinKey()->getStamp()), joinDef->getJoinKey()->getFieldName());

    auto keyVariableAttributeDeclaration =
        context->code->structDeclaratonInputTuple.getVariableDeclaration(joinDef->getJoinKey()->getFieldName());
    auto keyVariableAttributeStatement =
        VarDeclStatement(keyVariableDeclaration)
            .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                VarRef(keyVariableAttributeDeclaration)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(keyVariableAttributeStatement));

    // get key handle for current key
    // auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(keyHandlerVariableStatement));

    // access window slice state from state variable via key
    // auto windowState = key_value_handle.value();
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
        tf->createValueType(DataTypeFactory::createBasicValue(joinDef->getJoinKey()->getStamp(), "0"))));

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(windowStateVariableStatement));

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (joinDef->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        //      auto current_ts = NES::Windowing::getTsFromClock();
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));
    } else {
        //      auto current_ts = inputTuples[recordIndex].time //the time value of the key
        auto tsVariableDeclaration = context->code->structDeclaratonInputTuple.getVariableDeclaration(
            joinDef->getWindowType()->getTimeCharacteristic()->getField()->name);
        auto tsVariableDeclarationStatement =
            VarDeclStatement(currentTimeVariableDeclaration)
                .assign(
                    VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                        VarRef(tsVariableDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(
            std::make_shared<BinaryOperatorStatement>(tsVariableDeclarationStatement));
    }

    // update slices
    // windowManager->sliceStream(current_ts, windowState);
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    // uint64_t current_slice_index = windowState->getSliceIndexByTs(current_ts);
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(currentSliceIndexVariableStatement));

    // get the partial aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration).assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    auto sum = Windowing::SumAggregationDescriptor::on(Attribute("value", BasicType::UINT64));
    GeneratableWindowAggregationPtr agg = GeneratableCountAggregation::create(sum);

    agg->compileLiftCombine(
        context->code->currentCodeInsertionPoint, partialRef, context->code->structDeclaratonInputTuple,
        VarRef(context->code->varDeclarationInputTuples)[VarRefStatement(VarRef(*(context->code->varDeclarationRecordIndex)))]);

    // joinHandler->trigger();
    switch (joinDef->getTriggerPolicy()->getPolicyType()) {
        case Windowing::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowJoinVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("CCodeGenerator: Generate code for" << context->pipelineName << ": "
                                                  << " with code=" << context->code);

    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdater(context, windowJoinVariableDeclration);
    return true;
}
bool CCodeGenerator::generateCodeForCombiningWindow(Windowing::LogicalWindowDefinitionPtr window,
                                                    GeneratableWindowAggregationPtr generatableWindowAggregation,
                                                    PipelineContextPtr context, uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for combine window " << window);
    auto code = context->code;

    if (window->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Type::Combining) {
        context->pipelineName = "combiningWindowType";
    } else {
        context->pipelineName = "sliceMergingWindowType";
    }

    auto debugDecl = VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), context->pipelineName);
    auto debState = VarDeclStatement(debugDecl).assign(
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))));
    context->code->variableInitStmts.push_back(std::make_shared<BinaryOperatorStatement>(debState));

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");
    auto windowOperatorHandlerDeclaration = getWindowOperatorHandler(context, context->code->varDeclarationExecutionContext, windowOperatorIndex);
    auto windowHandlerVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");

    auto keyStamp = window->isKeyed() ? window->getOnKey()->getStamp():  window->getWindowAggregation()->on()->getStamp();

    auto getWindowHandlerStatement = getAggregationWindowHandler(
        windowOperatorHandlerDeclaration, keyStamp,
        window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
        window->getWindowAggregation()->getFinalAggregateStamp());
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());
    /*
    auto windowHandlerVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");

    NES_ASSERT(!window->getWindowAggregation()->getInputStamp()->isUndefined(), "window input type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getPartialAggregateStamp()->isUndefined(), "window partial type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getFinalAggregateStamp()->isUndefined(), "window final type is undefined");

    if (window->isKeyed()) {
        auto getWindowHandlerStatement = getAggregationWindowHandler(
            context->code->varDeclarationExecutionContext, window->getOnKey()->getStamp(),
            window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
            window->getWindowAggregation()->getFinalAggregateStamp());
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());
    } else {
        auto getWindowHandlerStatement = getAggregationWindowHandler(
            context->code->varDeclarationExecutionContext, window->getWindowAggregation()->on()->getStamp(),
            window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
            window->getWindowAggregation()->getFinalAggregateStamp());
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());
    }
*/
    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    // set result schema to context
    // generate result tuple struct

    // get allowed lateness
    //    auto allowedLateness = windowManager->getAllowedLateness();
    auto latenessHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "allowedLateness");
    auto getAllowedLatenessStateVariable = FunctionCallStatement("getAllowedLateness");
    auto allowedLatenessHandlerVariableStatement = VarRef(windowManagerVarDeclaration).accessPtr(getAllowedLatenessStateVariable);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(latenessHandlerVariableDeclaration).assign(allowedLatenessHandlerVariableStatement).copy());

    // initiate maxWatermark variable
    // auto maxWatermark = 0;
    auto maxWatermarkVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), "maxWatermark");
    auto maxWatermarkInitStatement =
        VarDeclStatement(maxWatermarkVariableDeclaration)
            .assign(Constant(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))));
    context->code->variableInitStmts.push_back(std::make_shared<BinaryOperatorStatement>(maxWatermarkInitStatement));

    /**
   * within the loop
   */
    // get the value for current watermark
    // auto currentWatermark = record[index].ts - 0;
    auto currentWatermarkVariableDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), "currentWatermark");
    auto tsVariableDeclaration = context->code->structDeclaratonInputTuple.getVariableDeclaration("end");
    auto calculateMaxTupleStatement =
        VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
            VarRef(tsVariableDeclaration))
            * ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(
                BasicType::UINT64,
                std::to_string(window->getWindowType()->getTimeCharacteristic()->getTimeUnit().getMultiplier()))))
        - Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0))));
    auto currentWatermarkStatement = VarDeclStatement(currentWatermarkVariableDeclaration).assign(calculateMaxTupleStatement);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(currentWatermarkStatement));

    //get min watermark
    auto minWatermarkVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "minWatermark");
    auto getMinWatermarkStateVariable = FunctionCallStatement("getMinWatermark");
    auto minWatermarkHandlerVariableStatement =
        VarDeclStatement(minWatermarkVariableDeclaration)
            .assign(VarRef(windowHandlerVariableDeclration).accessPtr(getMinWatermarkStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(minWatermarkHandlerVariableStatement));

    //        if (ts < (minWatermark - allowedLatness)
    //{continue;}
    auto ifStatementAllowedLateness = IF(VarRef(currentWatermarkVariableDeclaration) < VarRef(minWatermarkVariableDeclaration)
                                                 - VarRef(latenessHandlerVariableDeclaration),
                                         Continue());
    context->code->currentCodeInsertionPoint->addStatement(ifStatementAllowedLateness.createCopy());

    // Check and update max watermark if current watermark is greater than maximum watermark
    // if (currentWatermark > maxWatermark) {
    //     maxWatermark = currentWatermark;
    // };
    auto updateMaxWatermarkStatement =
        VarRef(maxWatermarkVariableDeclaration).assign(VarRef(currentWatermarkVariableDeclaration));
    auto ifStatement =
        IF(VarRef(currentWatermarkVariableDeclaration) > VarRef(maxWatermarkVariableDeclaration), updateMaxWatermarkStatement);
    context->code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());

    //        NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>* state_variable = (NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>*) state_var;
    auto stateVariableDeclaration = VariableDeclaration::create(
        tf->createPointer(tf->createAnonymusDataType("NES::StateVariable<int64_t, NES::Windowing::WindowSliceStore<int64_t>*>")),
        "state_variable");

    auto stateVarDeclarationStatement =
        VarDeclStatement(stateVariableDeclaration)
            .assign(TypeCast(VarRef(windowStateVarDeclaration), stateVariableDeclaration.getDataType()));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(stateVarDeclarationStatement));

    // Read key value from record
    //        int64_t key = windowTuples[recordIndex].key;
    DataTypePtr dataType;
    if (window->isKeyed()) {
        dataType = window->getOnKey()->getStamp();
    } else {
        dataType = window->getWindowAggregation()->on()->getStamp();
    }

    //TODO this is not nice but we cannot create an empty one or a ptr
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt64()), "key");
    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration =
            context->code->structDeclaratonInputTuple.getVariableDeclaration(window->getOnKey()->getFieldName());
        auto keyVariableAttributeStatement =
            VarDeclStatement(keyVariableDeclaration)
                .assign(
                    VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                        VarRef(keyVariableAttributeDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(
            std::make_shared<BinaryOperatorStatement>(keyVariableAttributeStatement));
    } else {
        auto defaultKeyAssignment = VarDeclStatement(keyVariableDeclaration)
                                        .assign(ConstantExpressionStatement(tf->createValueType(
                                            DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(defaultKeyAssignment));
    }

    // get key handle for current key
    //        auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(stateVariableDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(keyHandlerVariableStatement));

    //    auto windowState = key_value_handle.valueOrDefault(0);
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(
        ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(windowStateVariableStatement));

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "start");
    if (window->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        auto getCurrentTsStatement =
            VarDeclStatement(currentTimeVariableDeclaration)
                .assign(
                    VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                        VarRef(currentTimeVariableDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));
    } else {
        currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "start");
        auto tsVariableDeclarationStatement =
            VarDeclStatement(currentTimeVariableDeclaration)
                .assign(
                    VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                        VarRef(currentTimeVariableDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(
            std::make_shared<BinaryOperatorStatement>(tsVariableDeclarationStatement));
    }
    // get current timestamp
    // TODO add support for event time

    auto currentCntVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "cnt");
    auto getCurrentCntStatement =
        VarDeclStatement(currentCntVariable)
            .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                VarRef(currentCntVariable)));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentCntStatement));

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(currentSliceIndexVariableStatement));

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration).assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    generatableWindowAggregation->compileLiftCombine(
        context->code->currentCodeInsertionPoint, partialRef, context->code->structDeclaratonInputTuple,
        VarRef(context->code->varDeclarationInputTuples)[VarRefStatement(VarRef(*(context->code->varDeclarationRecordIndex)))]);

    // get the slice metadata aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getSliceMetadata = FunctionCallStatement("getSliceMetadata");
    auto getSliceMetadataCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceMetadata);
    VariableDeclaration sliceMetadataDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "sliceMetaData");
    auto sliceAssigment = VarDeclStatement(sliceMetadataDeclaration).assign(getSliceMetadataCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(sliceAssigment));

    auto getSliceCall = FunctionCallStatement("incrementRecordsPerSliceByValue");
    getSliceCall.addParameter(VarRef(currentCntVariable));
    auto updateSliceStatement = VarRef(sliceMetadataDeclaration)[current_slice_ref].accessRef(getSliceCall);
    context->code->currentCodeInsertionPoint->addStatement(updateSliceStatement.createCopy());

    // windowHandler->trigger();
    switch (window->getTriggerPolicy()->getPolicyType()) {
        case Windowing::triggerOnRecord: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->currentCodeInsertionPoint->addStatement(call);
            break;
        }
        case Windowing::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    // set the watermark of input buffer based on maximum watermark
    // inputTupleBuffer.setWatermark(maxWatermark);
    auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
    setWatermarkFunctionCall.addParameter(VarRef(maxWatermarkVariableDeclaration));
    auto setWatermarkStatement = VarRef(context->code->varDeclarationInputBuffer).accessRef(setWatermarkFunctionCall);
    context->code->cleanupStmts.push_back(setWatermarkStatement.createCopy());

    NES_DEBUG("CCodeGenerator: Generate code for" << context->pipelineName << ": "
                                                  << " with code=" << context->code);

    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdater(context, windowHandlerVariableDeclration);
    return true;
}

uint64_t CCodeGenerator::generateWindowSetup(Windowing::LogicalWindowDefinitionPtr window, PipelineContextPtr context) {
    auto tf = getTypeFactory();

    auto executionContextRef= VarRefStatement(context->code->varDeclarationExecutionContext);
    auto windowOperatorHandler = Windowing::WindowOperatorHandler::create(window, context->getResultSchema());
    auto windowOperatorIndex = context->registerOperatorHandler(windowOperatorHandler);


    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::WindowOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
        .assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
        .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());



    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement = VarDeclStatement(resultSchemaDeclaration)
        .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());


    auto keyStamp = window->isKeyed() ?
                    window->getOnKey()->getStamp():
                    window->getWindowAggregation()->on()->getStamp();
    auto keyType = tf->createDataType(keyStamp);

    auto aggregation = window->getWindowAggregation();
    auto aggregationInputType = tf->createDataType(aggregation->getInputStamp());
    auto partialAggregateType = tf->createDataType(aggregation->getPartialAggregateStamp());
    auto finalAggregateType = tf->createDataType(aggregation->getFinalAggregateStamp());
    auto executableAggregation = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "aggregation");
    FunctionCallStatementPtr createAggregateCall;
    if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Sum) {
        createAggregateCall =
            call("Windowing::ExecutableSumAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
    }
    auto statement = VarDeclStatement(executableAggregation).assign(createAggregateCall);
    setupScope->addStatement(statement.copy());

    auto policy = window->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode=" << policy->getPolicyType() << " not implemented");
    }

    auto action = window->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Windowing::WindowAggregationTriggerAction) {
        auto createTriggerActionCall = call("Windowing::ExecutableCompleteAggregationTriggerAction<" + keyType->getCode()->code_ + ","
                                                         + aggregationInputType->getCode()->code_ + "," + partialAggregateType->getCode()->code_ + ","
                                                         + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        createTriggerActionCall->addParameter(VarRef(resultSchemaDeclaration));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else if (action->getActionType() == Windowing::SliceAggregationTriggerAction) {
        auto createTriggerActionCall = call("Windowing::ExecutableSliceAggregationTriggerAction<" + keyType->getCode()->code_ + ","
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
    auto createAggregationWindowHandlerCall = call("Windowing::AggregationWindowHandler<"
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
    auto setWindowHandlerCall = call("setWindowHandler");
    setWindowHandlerCall->addParameter(VarRef(windowHandler));
    auto setWindowHandlerStatement = VarRef(windowOperatorHandlerDeclaration).accessPtr(setWindowHandlerCall);
    setupScope->addStatement(setWindowHandlerStatement.copy());

    // setup window handler
    auto getSharedFromThis = call("shared_from_this");
    auto setUpWindowHandlerCall = call("setup");
    setUpWindowHandlerCall->addParameter(VarRef(context->code->varDeclarationExecutionContext).accessRef(getSharedFromThis));

    auto setupWindowHandlerStatement = VarRef(windowHandler).accessPtr(setUpWindowHandlerCall);
    setupScope->addStatement(setupWindowHandlerStatement.copy());



    return windowOperatorIndex;
}

std::string CCodeGenerator::generateCode(PipelineContextPtr context) {
    auto code = context->code;
    // FunctionDeclaration main_function =
    auto tf = getTypeFactory();
    auto setupFunction = FunctionDefinition::create("setup")
                             ->addParameter(code->varDeclarationExecutionContext)
                             ->returns(tf->createDataType(DataTypeFactory::createUInt32()));


    for(auto setupScope: context->getSetupScopes()){
        setupFunction->addStatement(setupScope);
    }

    setupFunction->addStatement(ReturnStatement::create(Constant(
        tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))).createCopy()));


    auto startFunction = FunctionDefinition::create("start")
        ->addParameter(code->varDeclarationExecutionContext)
        ->returns(tf->createDataType(DataTypeFactory::createUInt32()));

    for(auto startScope: context->getStartScopes()){
        startFunction->addStatement(startScope);
    }

    startFunction->addStatement(ReturnStatement::create(Constant(
        tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))).createCopy()));


    auto functionBuilder = FunctionDefinition::create("execute")
                                          ->returns(tf->createDataType(DataTypeFactory::createUInt32()))
                                          ->addParameter(code->varDeclarationInputBuffer)
                                          ->addParameter(code->varDeclarationExecutionContext)
                                          ->addParameter(code->varDeclarationWorkerContext);
    code->variableDeclarations.push_back(code->varDeclarationNumberOfResultTuples);
    for (auto& variableDeclaration : code->variableDeclarations) {
        functionBuilder->addVariableDeclaration(variableDeclaration);
    }
    for (auto& variableStatement : code->variableInitStmts) {
        functionBuilder->addStatement(variableStatement);
    }

    /* here comes the code for the processing loop */
    functionBuilder->addStatement(code->forLoopStmt);

    /* add statements executed after the for loop, for example cleanup code */
    for (auto& stmt : code->cleanupStmts) {
        functionBuilder->addStatement(stmt);
    }

    /* add return statement */
    functionBuilder->addStatement(code->returnStmt);

    FileBuilder fileBuilder = FileBuilder::create("query.cpp");
    /* add core declarations */
    fileBuilder.addDeclaration(code->structDeclaratonInputTuple.copy());
    /* add generic declarations by operators*/
    for (auto& typeDeclaration : code->typeDeclarations) {
        fileBuilder.addDeclaration(typeDeclaration.copy());
    }


    auto executablePipeline = ClassDefinition::create("ExecutablePipelineStage" + context->pipelineName);
    executablePipeline->addBaseClass("NodeEngine::Execution::ExecutablePipelineStage");
    executablePipeline->addMethod(ClassDefinition::Public, functionBuilder);
    executablePipeline->addMethod(ClassDefinition::Public, setupFunction);

    auto executablePipelineDeclaration = executablePipeline->getDeclaration();
    auto pipelineNamespace = NamespaceDefinition::create("NES");
    pipelineNamespace->addDeclaration(executablePipelineDeclaration);



    auto createFunction = FunctionDefinition::create("create");
    auto returnStatement = ReturnStatement::create(SharedPointerGen::makeShared(executablePipelineDeclaration->getType()));
    createFunction->addStatement(returnStatement);

    createFunction->returns(SharedPointerGen::createSharedPtrType( CompilerTypesFactory().createAnonymusDataType("NodeEngine::Execution::ExecutablePipelineStage")));
    pipelineNamespace->addDeclaration(createFunction->getDeclaration());
    CodeFile file  =  fileBuilder
        .addDeclaration(pipelineNamespace->getDeclaration()).build();

    return file.code;
}



NodeEngine::Execution::ExecutablePipelineStagePtr CCodeGenerator::compile(PipelineContextPtr code) {
    std::string src = generateCode(code);
    auto compiledCode = compiler->compile(src, true /*debugging flag replace later*/);
    return CompiledExecutablePipelineStage::create(compiledCode);
}

BinaryOperatorStatement CCodeGenerator::allocateTupleBuffer(VariableDeclaration pipelineContext) {
    auto allocateTupleBuffer = FunctionCallStatement("allocateTupleBuffer");
    return VarRef(pipelineContext).accessRef(allocateTupleBuffer);
}

BinaryOperatorStatement CCodeGenerator::getBufferSize(VariableDeclaration tupleBufferVariable) {
    auto getBufferSizeFunctionCall = FunctionCallStatement("getBufferSize");
    return VarRef(tupleBufferVariable).accessRef(getBufferSizeFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setNumberOfTuples(VariableDeclaration tupleBufferVariable,
                                                          VariableDeclaration numberOfResultTuples) {
    auto setNumberOfTupleFunctionCall = FunctionCallStatement("setNumberOfTuples");
    setNumberOfTupleFunctionCall.addParameter(VarRef(numberOfResultTuples));
    /* set number of output tuples to result buffer */
    return VarRef(tupleBufferVariable).accessRef(setNumberOfTupleFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setWatermark(VariableDeclaration tupleBufferVariable,
                                                     VariableDeclaration inputBufferVariable) {
    auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
    setWatermarkFunctionCall.addParameter(getWatermark(inputBufferVariable));
    /* copy watermark */
    return VarRef(tupleBufferVariable).accessRef(setWatermarkFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setOriginId(VariableDeclaration tupleBufferVariable,
                                                    VariableDeclaration inputBufferVariable) {
    auto setOriginIdFunctionCall = FunctionCallStatement("setOriginId");
    setOriginIdFunctionCall.addParameter(getOriginId(inputBufferVariable));
    /* copy watermark */
    return VarRef(tupleBufferVariable).accessRef(setOriginIdFunctionCall);
}

CCodeGenerator::~CCodeGenerator(){};

BinaryOperatorStatement CCodeGenerator::emitTupleBuffer(VariableDeclaration pipelineContext,
                                                        VariableDeclaration tupleBufferVariable,
                                                        VariableDeclaration workerContextVariable) {
    auto emitTupleBuffer = FunctionCallStatement("emitBuffer");
    emitTupleBuffer.addParameter(VarRef(tupleBufferVariable));
    emitTupleBuffer.addParameter(VarRef(workerContextVariable));
    return VarRef(pipelineContext).accessRef(emitTupleBuffer);
}
BinaryOperatorStatement CCodeGenerator::getBuffer(VariableDeclaration tupleBufferVariable) {
    auto getBufferFunctionCall = FunctionCallStatement("getBuffer");
    return VarRef(tupleBufferVariable).accessRef(getBufferFunctionCall);
}
BinaryOperatorStatement CCodeGenerator::getWatermark(VariableDeclaration tupleBufferVariable) {
    auto getWatermarkFunctionCall = FunctionCallStatement("getWatermark");
    return VarRef(tupleBufferVariable).accessRef(getWatermarkFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::getOriginId(VariableDeclaration tupleBufferVariable) {
    auto getWatermarkFunctionCall = FunctionCallStatement("getOriginId");
    return VarRef(tupleBufferVariable).accessRef(getWatermarkFunctionCall);
}

#define TO_CODE(type) tf->createDataType(type)->getCode()->code_

BinaryOperatorStatement CCodeGenerator::getAggregationWindowHandler(VariableDeclaration pipelineContextVariable,
                                                                    DataTypePtr keyType, DataTypePtr inputType,
                                                                    DataTypePtr partialAggregateType,
                                                                    DataTypePtr finalAggregateType) {
    auto tf = getTypeFactory();
    auto call = FunctionCallStatement(std::string("getWindowHandler<NES::Windowing::AggregationWindowHandler, ")
                                      + TO_CODE(keyType) + ", " + TO_CODE(inputType) + "," + TO_CODE(partialAggregateType) + ","
                                      + TO_CODE(finalAggregateType) + " >");
    return VarRef(pipelineContextVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getJoinWindowHandler(VariableDeclaration pipelineContextVariable, DataTypePtr KeyType) {
    auto tf = getTypeFactory();
    auto call = FunctionCallStatement(std::string("getJoinHandler<NES::Join::JoinHandler, ") + TO_CODE(KeyType) + " >");
    return VarRef(pipelineContextVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getStateVariable(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getTypedWindowState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getLeftJoinState(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getLeftJoinState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getRightJoinState(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getRightJoinState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getWindowManager(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getWindowManager");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

TypeCastExprStatement CCodeGenerator::getTypedBuffer(VariableDeclaration tupleBufferVariable,
                                                     StructDeclaration structDeclaraton) {
    auto tf = getTypeFactory();
    return TypeCast(getBuffer(tupleBufferVariable), tf->createPointer(tf->createUserDefinedType(structDeclaraton)));
}
VariableDeclaration CCodeGenerator::getWindowOperatorHandler(PipelineContextPtr context, VariableDeclaration tupleBufferVariable, uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto executionContextRef= VarRefStatement(tupleBufferVariable);
    auto windowOperatorHandlerDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::WindowOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
        .assign(executionContextRef.accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    return windowOperatorHandlerDeclaration;
}

VariableDeclaration CCodeGenerator::getJoinOperatorHandler(PipelineContextPtr context, VariableDeclaration tupleBufferVariable, uint64_t joinOperatorIndex) {
    auto tf = getTypeFactory();
    auto executionContextRef= VarRefStatement(tupleBufferVariable);
    auto windowOperatorHandlerDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::JoinOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
        .assign(executionContextRef.accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    return windowOperatorHandlerDeclaration;
}
}// namespace NES