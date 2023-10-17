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

#include <Common/DataTypes/ArrayType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/Expressions/FieldRenameExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ClassDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ConstructorDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/NamespaceDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Runtime/SharedPointerGen.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ContinueStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/IFElseStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/PredicatedFilterStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <Util/StdInt.hpp>
#include <Util/magicenum/magic_enum.hpp>
#ifdef TFDEF
#include <QueryCompiler/CodeGenerator/CCodeGenerator/TensorflowAdapter.hpp>
#endif//TFDEF

#include <API/Expressions/Expressions.hpp>
#include <Operators/LogicalOperators/InferModelOperatorHandler.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipelineStage.hpp>
#include <QueryCompiler/Compiler/LazyCompiledExecutablePipelineStage.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/PointerDataType.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/CEPOperatorHandler.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Operators/LogicalOperators/Windows/Actions/BaseJoinActionDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Actions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Operators/LogicalOperators/Windows/TriggerPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/TriggerPolicies/OnTimeTriggerPolicyDescription.hpp>

namespace NES::QueryCompilation {
CCodeGenerator::CCodeGenerator() {}

#define TO_CODE(type) tf->createDataType(type)->getCode()->code_

StructDeclaration CCodeGenerator::getStructDeclarationFromSchema(const std::string& structName, const SchemaPtr& schema) {
    auto tf = getTypeFactory();
    /* struct definition for tuples */
    StructDeclaration structDeclarationTuple = StructDeclaration::create(structName, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();

    NES_DEBUG("Converting Schema: {}", schema->toString());
    NES_DEBUG("Define Struct : {}", structName);

    for (uint64_t i = 0; i < schema->getSize(); ++i) {
        if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
            structDeclarationTuple.addField(
                VariableDeclaration::create(schema->get(i)->getDataType(), schema->get(i)->getName()));
        } else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
            auto valuePointer = GeneratableTypesFactory::createPointer(tf->createDataType(schema->get(i)->getDataType()));
            auto valuePointerDeclaration = VariableDeclaration::create(valuePointer, schema->get(i)->getName());
            structDeclarationTuple.addField(valuePointerDeclaration);
        } else {
            NES_ERROR("inputSchema->getLayoutType()is neither ROW_LAYOUT nor COL_LAYOUT!!!");
        }
        NES_DEBUG("Field {}: {} {}", i, schema->get(i)->getDataType()->toString(), schema->get(i)->getName());
    }
    return structDeclarationTuple;
}

VariableDeclarationPtr getVariableDeclarationForField(const StructDeclaration& structDeclaration,
                                                      const AttributeFieldPtr& field) {
    if (structDeclaration.getField(field->getName())) {
        return std::make_shared<VariableDeclaration>(structDeclaration.getVariableDeclaration(field->getName()));
    }
    return VariableDeclarationPtr();
}

std::string toString(void*, const DataTypePtr&) { return ""; }

CodeGeneratorPtr CCodeGenerator::create() { return std::make_shared<CCodeGenerator>(); }

bool CCodeGenerator::generateCodeForScanSetup(PipelineContextPtr context) {
    auto tf = getTypeFactory();
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    return true;
}

bool CCodeGenerator::generateCodeForScan(SchemaPtr inputSchema, SchemaPtr outputSchema, PipelineContextPtr context) {
    NES_DEBUG("CCodeGenerator: Generating code for scan with inputSchema {}", inputSchema->toString());
    context->inputSchema = outputSchema->copy();
    auto code = context->code;
    switch (context->arity) {
        case PipelineContext::PipelineContextArity::Unary: {
            // it is assumed that the input item is the first element!
            // so place to front
            // todo remove this assumption
            code->structDeclarationInputTuples.insert(code->structDeclarationInputTuples.begin(),
                                                      getStructDeclarationFromSchema("InputTuple", inputSchema));
            NES_DEBUG("arity unary generate scan for input={} output={}", inputSchema->toString(), outputSchema->toString());
            break;
        }
        case PipelineContext::PipelineContextArity::BinaryLeft: {
            code->structDeclarationInputTuples.emplace_back(getStructDeclarationFromSchema("InputTupleLeft", inputSchema));
            NES_DEBUG("arity binaryleft generate scan for input={} output={}", inputSchema->toString(), outputSchema->toString());
            break;
        }
        case PipelineContext::PipelineContextArity::BinaryRight: {
            code->structDeclarationInputTuples.emplace_back(getStructDeclarationFromSchema("InputTupleRight", inputSchema));
            NES_DEBUG("arity binaryright generate scan for input={} output={}",
                      inputSchema->toString(),
                      outputSchema->toString());
            break;
        }
    }

    /** === set the result tuple depending on the input tuple===*/
    context->resultSchema = outputSchema;
    code->structDeclarationResultTuple = getStructDeclarationFromSchema("ResultTuple", outputSchema);
    auto tf = getTypeFactory();
    /* === declarations === */
    auto tupleBufferType = tf->createAnonymusDataType("NES::Runtime::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("Runtime::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer =
        VariableDeclaration::create(tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    VariableDeclaration varDeclarationWorkerContext =
        VariableDeclaration::create(tf->createReference(workerContextType), "workerContext");

    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationWorkerContext = varDeclarationWorkerContext;

    /* declaration of record index; */
    code->varDeclarationRecordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "recordIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());

    /*  declaration of num of records */

    if (inputSchema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        NES_DEBUG("CCodeGenerator::generateCodeForEmit: generate emit for row layout");
        code->varDeclarationInputTuples =
            VariableDeclaration::create(tf->createPointer(tf->createUserDefinedType(code->structDeclarationInputTuples[0])),
                                        "inputTuples");
    } else if (inputSchema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        NES_DEBUG("CCodeGenerator::generateCodeForEmit: generate emit for row layout");
        code->varDeclarationInputTuples =
            VariableDeclaration::create(tf->createUserDefinedType(code->structDeclarationInputTuples[0]), "inputTuples");
        auto varDeclInputTupleStmt = VarDeclStatement(code->varDeclarationInputTuples);
        NES_DEBUG("CCodeGenerator::generateCodeForEmit: varDeclResultTuple code is {}", varDeclInputTupleStmt.getCode()->code_);
        code->variableInitStmts.push_back(varDeclInputTupleStmt.copy());
    } else {
        NES_ERROR("inputSchema->getLayoutType()is neither ROW_LAYOUT nor COL_LAYOUT!!!");
    }
    /* ExecutionResult ret = Ok; */
    // TODO probably it's not safe that we can mix enum values with int32 but it is a good hack for me :P
    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createAnonymusDataType("ExecutionResult"),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "ExecutionResult::Ok"))
            .copy());
    code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));

    // If it is a row layout, then map struct to buffer, otherwise set the start of all fields
    if (inputSchema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        // Generates: InputTuple* inputTuples = (InputTuple*) inputTupleBuffer.getBuffer();
        code->variableInitStmts.push_back(
            VarDeclStatement(code->varDeclarationInputTuples)
                .assign(getTypedBuffer(code->varDeclarationInputBuffer, code->structDeclarationInputTuples[0]))
                .copy());

    } else if (inputSchema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        auto compStatement = code->currentCodeInsertionPoint;
        generateCodeInitStructFieldsColLayout(inputSchema,
                                              tf,
                                              varDeclarationInputBuffer,
                                              code->structDeclarationInputTuples[0],
                                              context->code->varDeclarationInputTuples,
                                              code->variableInitStmts,
                                              "capacityScan");
    }

    /* numOfRecords = inputBuffer.getNumberOfTuples(); */
    auto numberOfRecords = VarRef(varDeclarationInputBuffer).accessRef(context->code->tupleBufferGetNumberOfTupleCall);
    code->variableInitStmts.push_back(VarDeclStatement(code->varDeclarationNumOfInputTuples).assign(numberOfRecords).copy());

    /* for (uint64_t recordIndex = 0; recordIndex < tuple_buffer_1->num_tuples; ++id) */
    // input_buffer.getNumberOfTuples()
    code->forLoopStmt =
        std::make_shared<FOR>(code->varDeclarationRecordIndex,
                              (VarRef(code->varDeclarationRecordIndex) < VarRef(code->varDeclarationNumOfInputTuples)).copy(),
                              (++VarRef(code->varDeclarationRecordIndex)).copy());

    code->currentCodeInsertionPoint = code->forLoopStmt->getCompoundStatement();
    if (context->arity != PipelineContext::PipelineContextArity::Unary) {
        NES_DEBUG("adding in scan for schema={} context={}", inputSchema->toString(), context->inputSchema->toString());
    }

    auto recordHandler = context->getRecordHandler();
    if (inputSchema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        for (const AttributeFieldPtr& field : inputSchema->fields) {
            auto variable = getVariableDeclarationForField(code->structDeclarationInputTuples[0], field);
            auto fieldRefStatement =
                VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                    VarRef(variable));
            recordHandler->registerAttribute(field->getName(), fieldRefStatement.copy());
        }
    } else if (inputSchema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        for (const AttributeFieldPtr& field : inputSchema->fields) {
            auto variable = getVariableDeclarationForField(code->structDeclarationInputTuples[0], field);
            auto fieldRefStatement = VarRef(context->code->varDeclarationInputTuples)
                                         .accessRef(VarRef(variable))[VarRef(context->code->varDeclarationRecordIndex)];
            recordHandler->registerAttribute(field->getName(), fieldRefStatement.copy());
        }
    } else {
        NES_ERROR("inputSchema->getLayoutType()is neither ROW_LAYOUT nor COL_LAYOUT!!!");
    }

    code->returnStmt = ReturnStatement::create(VarRefStatement(*code->varDeclarationReturnValue).createCopy());
    return true;
}

void CCodeGenerator::generateCodeInitStructFieldsColLayout(const SchemaPtr& schema,
                                                           CompilerTypesFactoryPtr& tf,
                                                           const VariableDeclaration& varDeclarationBuffer,
                                                           const StructDeclaration structDeclaration,
                                                           const VariableDeclaration varTuples,
                                                           std::vector<StatementPtr>& statements,
                                                           const std::string& capacityVarName) {

    // Creating a layout from the schema
    auto layout = Runtime::MemoryLayouts::ColumnLayout::create(schema, false);

    // Creating capacity variable
    auto capacityVarDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt64()), capacityVarName);
    auto capacityVarStatement = getBufferSize(varDeclarationBuffer)
        / Constant(tf->createValueType(DataTypeFactory::createBasicValue(layout->getTupleSize())));
    auto capacityVarAssignment = VarDeclStatement(capacityVarDeclaration).assign(capacityVarStatement);
    statements.push_back(capacityVarAssignment.copy());

    uint64_t offsetCounter = 0;
    for (size_t i = 0; i < layout->getFieldSizes().size(); ++i) {
        const auto& field = schema->fields[i];
        const auto& fieldSize = layout->getFieldSizes()[i];
        auto variable = getVariableDeclarationForField(structDeclaration, field);
        auto fieldRefStmt = VarRef(varTuples).accessRef(VarRef(variable));

        // Create offSet in tupleBuffer
        auto lhs =
            TypeCast(getBuffer(varDeclarationBuffer), tf->createPointer(tf->createDataType(DataTypeFactory::createUInt8())));
        auto expr = BinaryOperatorStatement(lhs,
                                            BinaryOperatorType::PLUS_OP,
                                            Constant(tf->createValueType(DataTypeFactory::createBasicValue(offsetCounter))))
                        .addRight(BinaryOperatorType::MULTIPLY_OP, VarRef(capacityVarDeclaration), BracketMode::BRACKETS);
        auto offSetAssignment = TypeCast(expr, tf->createPointer(tf->createDataType(field->getDataType())));
        statements.push_back(fieldRefStmt.assign(offSetAssignment).copy());

        offsetCounter += fieldSize;
    }
}

bool CCodeGenerator::generateCodeForProjection(std::vector<ExpressionNodePtr> projectExpressions, PipelineContextPtr context) {
    auto recordHandler = context->getRecordHandler();
    for (const auto& expression : projectExpressions) {
        if (expression->instanceOf<FieldRenameExpressionNode>()) {
            // if rename expression add the attribute to the record handler.
            auto fieldRenameExpression = expression->as<FieldRenameExpressionNode>();
            auto originalAttribute = fieldRenameExpression->getOriginalField();
            if (!recordHandler->hasAttribute(originalAttribute->getFieldName())) {
                NES_FATAL_ERROR(
                    "CCodeGenerator: projection: the original attribute {} is not registered so we can not access it.",
                    originalAttribute->getFieldName());
            }
            // register the attribute with the new name in the record handler
            auto referenceToOriginalValue = recordHandler->getAttribute(originalAttribute->getFieldName());
            recordHandler->registerAttribute(fieldRenameExpression->getNewFieldName(), referenceToOriginalValue);
        } else if (expression->instanceOf<FieldAccessExpressionNode>()) {
            // it is a field access expression, so we just check if the record exists.
            auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
            if (!recordHandler->hasAttribute(fieldAccessExpression->getFieldName())) {
                NES_FATAL_ERROR("CCodeGenerator: projection: the attribute {} is not registered so we can not access it.",
                                fieldAccessExpression->getFieldName());
            }
        }
    }
    return true;
}

/**
 * generates code for predicates using branching
 * @param pred - defined predicate for the query
 * @param context - includes the context of the used fields
 * @param out - sending some other information if wanted
 * @return modified query-code
 */
bool CCodeGenerator::generateCodeForFilter(PredicatePtr pred, PipelineContextPtr context) {

    // create predicate expression from filter predicate
    auto predicateExpression = pred->generateCode(context->code, context->getRecordHandler());
    // create if statement
    auto ifStatement = IF(*predicateExpression);
    // update current compound_stmt
    // first, add the head and brackets of the if-statement
    context->code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());
    // second, move insertion point. the rest of the pipeline will be generated within the brackets of the if-statement
    context->code->currentCodeInsertionPoint = ifStatement.getCompoundStatement();
    return true;
}

/**
 * generates code for branchless predicates
 * @param pred - defined predicate for the query
 * @param context - includes the context of the used fields
 * @param out - sending some other information if wanted
 * @return modified query-code
 */
bool CCodeGenerator::generateCodeForFilterPredicated(PredicatePtr pred, PipelineContextPtr context) {

    // create predicate expression from filter predicate
    auto predicateExpression = pred->generateCode(context->code, context->getRecordHandler());
    auto predicatedFilter = PredicatedFilter(*predicateExpression,
                                             context->code->varDeclarationTuplePassesFilters,
                                             context->getTuplePassesFiltersIsDeclared());

    context->code->currentCodeInsertionPoint->addStatement(predicatedFilter.createCopy());
    context->setTrueTuplePassesFiltersIsDeclared();
    context->code->currentCodeInsertionPoint = predicatedFilter.getCompoundStatement();
    return true;
}

/**
 * @brief Code generation for an infer model operator
 * @return flag if the generation was successful.
 */
bool CCodeGenerator::generateCodeForInferModel(PipelineContextPtr context,
                                               std::vector<ExpressionItemPtr> inputFields,
                                               std::vector<ExpressionItemPtr> outputFields) {

    for (auto f : inputFields) {
        auto field = f->getExpressionNode()->as<FieldAccessExpressionNode>();
        auto attrField = AttributeField::create(field->getFieldName(), field->getStamp());
    }

    auto code = context->code;
    auto tf = getTypeFactory();
    auto recordHandler = context->getRecordHandler();
    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);
    int64_t inferModelOperatorHandlerIndex = 0;

    auto inferModelOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "inferModelOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<InferModel::InferModelOperatorHandler>");
    auto constantOperatorHandlerIndex =
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(inferModelOperatorHandlerIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(inferModelOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    code->variableInitStmts.push_back(windowOperatorStatement.copy());

    auto tensorflowDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "tensorflowAdapter");
    auto tensorflowDeclStatement =
        VarDeclStatement(tensorflowDeclaration).assign(call("inferModelOperatorHandler->getTensorflowAdapter"));
    code->variableInitStmts.push_back(tensorflowDeclStatement.copy());

    auto generateTensorFlowInferCall = call("tensorflowAdapter->infer");

    bool firstIter = false;
    std::shared_ptr<DataType> commonStamp;
    for (auto f : inputFields) {
        auto field = f->getExpressionNode()->as<FieldAccessExpressionNode>();
        if (!field->getStamp()->isNumeric() && !field->getStamp()->isBoolean()) {
            NES_ERROR("CCodeGenerator::generateCodeForInferModel: inputted data type for tensorflow model not supported: {}",
                      field->getStamp()->toString());
        }
        if (!firstIter) {
            commonStamp = field->getStamp();
        } else {
            commonStamp = commonStamp->join(field->getStamp());
        }
    }
    NES_DEBUG("CCodeGenerator::generateCodeForInferModel: Common stamp for input tensor: {}", commonStamp->toString());
    if (commonStamp->isInteger()) {
        generateTensorFlowInferCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(
            BasicType::UINT8,
            "BasicPhysicalType::NativeType::" + std::string(magic_enum::enum_name(BasicPhysicalType::NativeType::INT_64))))));
    } else if (commonStamp->isFloat()) {
        std::shared_ptr<Float> floatStamp = commonStamp->as<Float>(commonStamp);
        if (floatStamp->getBits() == 32) {
            generateTensorFlowInferCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(
                BasicType::UINT8,
                "BasicPhysicalType::NativeType::" + std::string(magic_enum::enum_name(BasicPhysicalType::NativeType::FLOAT))))));
        } else {
            generateTensorFlowInferCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(
                BasicType::UINT8,
                "BasicPhysicalType::NativeType::" + std::string(magic_enum::enum_name(BasicPhysicalType::NativeType::DOUBLE))))));
        }
    } else if (commonStamp->isBoolean()) {
        generateTensorFlowInferCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(
            BasicType::UINT8,
            "BasicPhysicalType::NativeType::" + std::string(magic_enum::enum_name(BasicPhysicalType::NativeType::BOOLEAN))))));
    } else {
        generateTensorFlowInferCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(
            BasicType::UINT8,
            "BasicPhysicalType::NativeType::" + std::string(magic_enum::enum_name(BasicPhysicalType::NativeType::UNDEFINED))))));
    }
    generateTensorFlowInferCall->addParameter(
        Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) inputFields.size()))));

    for (auto f : inputFields) {
        auto field = f->getExpressionNode()->as<FieldAccessExpressionNode>();
        auto attrField = AttributeField::create(field->getFieldName(), field->getStamp());
        if (commonStamp->isInteger()) {
            auto variableDeclaration = VariableDeclaration::create(DataTypeFactory::createInt64(), attrField->getName());
            generateTensorFlowInferCall->addParameter(
                VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                    VarRef(variableDeclaration)));
        } else if (commonStamp->isFloat()) {
            auto variableDeclaration = VariableDeclaration::create(DataTypeFactory::createDouble(), attrField->getName());
            generateTensorFlowInferCall->addParameter(
                VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                    VarRef(variableDeclaration)));
        } else if (commonStamp->isBoolean()) {
            auto variableDeclaration = VariableDeclaration::create(DataTypeFactory::createBoolean(), attrField->getName());
            generateTensorFlowInferCall->addParameter(
                VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                    VarRef(variableDeclaration)));
        } else {
            NES_ERROR("CCodeGenerator: common data type for tensorflow model not supported: {}", commonStamp->toString());
        }
    }

    code->currentCodeInsertionPoint->addStatement(generateTensorFlowInferCall);

    for (unsigned long i = 0; i < outputFields.size(); ++i) {
        auto field = outputFields.at(i)->getExpressionNode()->as<FieldAccessExpressionNode>();
        auto attrField = AttributeField::create(field->getFieldName(), field->getStamp());

        auto variableDeclaration = VariableDeclaration::create(DataTypeFactory::createFloat(), attrField->getName());
        auto attributeVariable = VarDeclStatement(variableDeclaration);

        auto getResultCall = call("tensorflowAdapter->getResultAt");
        getResultCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue((uint64_t) i))));
        auto assignedMap = attributeVariable.assign(getResultCall).copy();

        recordHandler->registerAttribute(attrField->getName(), VarRef(variableDeclaration).copy());
        code->currentCodeInsertionPoint->addStatement(assignedMap);
    }

    return true;
}

/**
 * generates code for a mapper with an defined AttributeField and a PredicatePtr
 * @param field - existing or new created field that includes the mapped function
 * @param pred - mapping function as a predicate tree for easy single lined functions.
 * @param context - includes the context of the used fields
 * @return modified query-code
 */
bool CCodeGenerator::generateCodeForMap(AttributeFieldPtr field, LegacyExpressionPtr pred, PipelineContextPtr context) {

    auto code = context->code;
    auto tf = getTypeFactory();

    // Check if the assignment value is new or if we have to create it
    auto mapExpression = pred->generateCode(code, context->getRecordHandler());
    auto recordHandler = context->getRecordHandler();
    if (recordHandler->hasAttribute(field->getName())) {
        // Get the attribute variable from the field and assign a new value to it.
        auto attributeVariable = recordHandler->getAttribute(field->getName());
        auto assignedMap = attributeVariable->assign(mapExpression);
        code->currentCodeInsertionPoint->addStatement(assignedMap.copy());
    } else {
        // Create a new attribute variable and assign the new value to it.
        auto variableDeclaration = VariableDeclaration::create(field->getDataType(), field->getName());
        auto attributeVariable = VarDeclStatement(variableDeclaration);
        auto assignedMap = attributeVariable.assign(mapExpression).copy();
        recordHandler->registerAttribute(field->getName(), VarRef(variableDeclaration).copy());
        code->currentCodeInsertionPoint->addStatement(assignedMap);
    }
    return true;
}

bool CCodeGenerator::generateCodeForEmit(SchemaPtr sinkSchema,
                                         OutputBufferAllocationStrategy bufferStrategy,
                                         OutputBufferAssignmentStrategy bufferAssignmentStrategy,
                                         PipelineContextPtr context) {

    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for Sink.");
    auto code = context->code;
    // set result schema to context
    context->resultSchema = sinkSchema;

    // generate result tuple struct
    auto structDeclarationResultTuple = getStructDeclarationFromSchema("ResultTuple", sinkSchema);
    // add type declaration for the result tuple
    code->typeDeclarations.push_back(structDeclarationResultTuple);

    if (bufferAssignmentStrategy == OutputBufferAssignmentStrategy::RECORD_COPY) {
        structDeclarationResultTuple = code->structDeclarationInputTuples[0];
    }

    if (sinkSchema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        VariableDeclaration varDeclResultTuple =
            VariableDeclaration::create(tf->createPointer(tf->createUserDefinedType(structDeclarationResultTuple)),
                                        "resultTuples");

        // initialize result buffer
        if (bufferStrategy == OutputBufferAllocationStrategy::ONLY_INPLACE_OPERATIONS) {
            // We do not even initialize a buffer, we just use "inputBuffer" as the resultBuffer-handle for the later emit.
            // The only contents in the Scan's for loop will be map operations.
            code->varDeclarationResultBuffer = code->varDeclarationInputBuffer;
            auto numberOfRecords =
                VarRef(code->varDeclarationInputBuffer).accessRef(context->code->tupleBufferGetNumberOfTupleCall);

        } else {
            // An optimization other than ONLY_INPLACE_OPERATIONS will be applied:

            auto recordHandler = context->getRecordHandler();
            auto tupleBufferType = tf->createAnonymusDataType("NES::Runtime::TupleBuffer");
            auto varDeclarationResultBuffer = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");
            code->varDeclarationResultBuffer = varDeclarationResultBuffer;

            if (bufferStrategy == OutputBufferAllocationStrategy::REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK
                || bufferStrategy
                    == OutputBufferAllocationStrategy::REUSE_INPUT_BUFFER) {// Reuse the input buffer as the result buffer.
                // Generates: NES::Runtime::TupleBuffer resultTupleBuffer = inputTupleBuffer;
                code->variableInitStmts.push_back(
                    VarDeclStatement(code->varDeclarationResultBuffer).assign(VarRef(code->varDeclarationInputBuffer)).copy());
                // Generates: ResultTuple* resultTuples = (ResultTuple*) resultTupleBuffer.getBuffer();
                code->variableInitStmts.push_back(
                    VarDeclStatement(varDeclResultTuple)
                        .assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple))
                        .copy());

                // Save all input fields that may get overwritten because of the input==result optimization into temporary variables.
                for (const auto& field : context->resultSchema->fields) {
                    if (context->getInputSchema()->hasFieldName(field->getName())) {
                        // check if record handler has current field
                        if (!recordHandler->hasAttribute(field->getName())) {
                            NES_FATAL_ERROR("CCodeGenerator: field: {} is part of the output schema, but not registered in the "
                                            "record handler.",
                                            field->toString());
                        }

                        std::string tmpVarName = "tmp_" + field->getName();
                        auto variableDeclaration = VariableDeclaration::create(field->getDataType(), tmpVarName);
                        auto attributeVariable = VarDeclStatement(variableDeclaration);
                        auto assignedTmpVar = attributeVariable.assign(recordHandler->getAttribute(field->getName())).copy();
                        recordHandler->registerAttribute(field->getName(), VarRef(variableDeclaration).copy());
                        code->currentCodeInsertionPoint->addStatement(assignedTmpVar);
                    }
                }
            } else {// Allocate a dedicated result buffer.
                // Generates: NES::Runtime::TupleBuffer resultTupleBuffer = workerContext.allocateTupleBuffer();
                code->variableInitStmts.push_back(VarDeclStatement(code->varDeclarationResultBuffer)
                                                      .assign(allocateTupleBuffer(code->varDeclarationWorkerContext))
                                                      .copy());
                // Generates: ResultTuple* resultTuples = (ResultTuple*) resultTupleBuffer.getBuffer();
                code->variableInitStmts.push_back(
                    VarDeclStatement(varDeclResultTuple)
                        .assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple))
                        .copy());
            }

            if (bufferAssignmentStrategy == OutputBufferAssignmentStrategy::FIELD_COPY) {
                // Now, copy all fields listed in the result schema into the result buffer.
                for (const auto& field : context->resultSchema->fields) {
                    auto resultRecordFieldVariableDeclaration =
                        getVariableDeclarationForField(structDeclarationResultTuple, field);
                    if (!resultRecordFieldVariableDeclaration) {
                        NES_FATAL_ERROR("CCodeGenerator: Could not extract field {} from result record struct {}",
                                        field->toString(),
                                        structDeclarationResultTuple.getTypeName());
                    }

                    // check if record handler has current field
                    if (!recordHandler->hasAttribute(field->getName())) {
                        NES_FATAL_ERROR("CCodeGenerator: field: {}"
                                        " is part of the output schema, "
                                        "but not registered in the record handler.",
                                        field->toString());
                    }

                    // Get current field from record handler.
                    auto currentFieldVariableReference = recordHandler->getAttribute(field->getName());

                    // use regular assign for types which are not arrays, as fixed char arrays support
                    // assignment by operator.
                    auto const copyFieldStatement = VarRef(varDeclResultTuple)[VarRef(code->varDeclarationNumberOfResultTuples)]
                                                        .accessRef(VarRef(resultRecordFieldVariableDeclaration))
                                                        .assign(currentFieldVariableReference);

                    code->currentCodeInsertionPoint->addStatement(copyFieldStatement.copy());
                }
            } else if (bufferAssignmentStrategy == OutputBufferAssignmentStrategy::RECORD_COPY) {
                auto recordCopyStatement = VarRef(varDeclResultTuple)[VarRef(code->varDeclarationNumberOfResultTuples)].assign(
                    VarRef(code->varDeclarationInputTuples)[VarRef(code->varDeclarationRecordIndex)]);
                code->currentCodeInsertionPoint->addStatement(recordCopyStatement.copy());
            }

            if (context->getTuplePassesFiltersIsDeclared()) {
                // the pipeline uses (branchless) predicated filtering
                // we increase "numberOfResultTuples" if all filters are passed, as shown by the boolean tuplePassesFilters
                code->currentCodeInsertionPoint->addStatement(
                    VarRef(code->varDeclarationNumberOfResultTuples)
                        .assign(VarRef(code->varDeclarationNumberOfResultTuples) + VarRef(code->varDeclarationTuplePassesFilters))
                        .copy());
            } else {
                // no (branchless) predicated filtering in pipeline
                // increment number of tuples in buffer -> ++numberOfResultTuples;
                code->currentCodeInsertionPoint->addStatement((++VarRef(code->varDeclarationNumberOfResultTuples)).copy());
            }

            // Generate logic to check if tuple buffer is already full. If so we emit the current one and pass it to the Runtime.
            // We can optimize this away if the result schema is smaller than the input schema.
            if (!(bufferStrategy == OutputBufferAllocationStrategy::REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK
                  || bufferStrategy == OutputBufferAllocationStrategy::OMIT_OVERFLOW_CHECK)) {
                generateTupleBufferSpaceCheck(context, varDeclResultTuple, structDeclarationResultTuple, sinkSchema);
            }
        }
    } else if (sinkSchema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {
        auto tupleBufferType = tf->createAnonymusDataType("NES::Runtime::TupleBuffer");
        auto varDeclarationResultBuffer = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");
        code->varDeclarationResultBuffer = varDeclarationResultBuffer;

        code->variableInitStmts.push_back(VarDeclStatement(code->varDeclarationResultBuffer)
                                              .assign(allocateTupleBuffer(code->varDeclarationWorkerContext))
                                              .copy());
        auto varDeclResultTuple =
            VariableDeclaration::create(tf->createUserDefinedType(structDeclarationResultTuple), "resultTuples");

        auto varDeclResultTupleStmt = VarDeclStatement(varDeclResultTuple);
        NES_DEBUG("CCodeGenerator::generateCodeForEmit: varDeclResultTuple code is {}", varDeclResultTupleStmt.getCode()->code_);
        code->variableInitStmts.push_back(varDeclResultTupleStmt.copy());

        // Setting the start of all fields for col layout
        generateCodeInitStructFieldsColLayout(sinkSchema,// TODO duplicate to maxTuple?
                                              tf,
                                              code->varDeclarationResultBuffer,
                                              code->structDeclarationResultTuple,
                                              varDeclResultTuple,
                                              code->variableInitStmts,
                                              "capacityEmit");

        auto recordHandler = context->getRecordHandler();
        for (const auto& field : context->resultSchema->fields) {

            auto resultRecordFieldVariableDeclaration = getVariableDeclarationForField(structDeclarationResultTuple, field);
            if (!resultRecordFieldVariableDeclaration) {
                NES_FATAL_ERROR("CodeGenerator: Could not extract field {} from result record struct {}",
                                field->toString(),
                                structDeclarationResultTuple.getTypeName());
            }

            // check if record handler has current field
            if (!recordHandler->hasAttribute(field->getName())) {
                NES_FATAL_ERROR("CCodeGenerator: field: {}"
                                " is part of the output schema, "
                                "but not registered in the record handler.",
                                field->toString());
            }

            // Get current field from record handler.
            auto currentFieldVariableReference = recordHandler->getAttribute(field->getName());
            auto const copyFieldStatement =
                VarRef(varDeclResultTuple)
                    .accessRef(VarRef(resultRecordFieldVariableDeclaration))[VarRef(code->varDeclarationNumberOfResultTuples)]
                    .assign(currentFieldVariableReference);

            code->currentCodeInsertionPoint->addStatement(copyFieldStatement.copy());
        }

        // Increment number of tuples in buffer -> ++numberOfResultTuples;
        code->currentCodeInsertionPoint->addStatement((++VarRef(code->varDeclarationNumberOfResultTuples)).copy());

        // Generate logic to check if tuple buffer is already full. If so we emit the current one and pass it to the Runtime.
        // We can optimize this away if the result schema is smaller than the input schema.
        if (!(bufferStrategy == OutputBufferAllocationStrategy::REUSE_INPUT_BUFFER_AND_OMIT_OVERFLOW_CHECK
              || bufferStrategy
                  == OutputBufferAllocationStrategy::OMIT_OVERFLOW_CHECK)) {// TODO does this work yet for col layout?
            generateTupleBufferSpaceCheck(context, varDeclResultTuple, structDeclarationResultTuple, sinkSchema);
        }
    } else {
        NES_ERROR("CCodeGenerator: inputSchema->getLayoutType() is neither ROW_LAYOUT nor COL_LAYOUT!!!");
    }

    // Generate final logic to emit the last buffer to the Runtime
    // 1. set the number of tuples to the buffer
    if (bufferStrategy != OutputBufferAllocationStrategy::ONLY_INPLACE_OPERATIONS) {
        code->cleanupStmts.push_back(
            setNumberOfTuples(code->varDeclarationResultBuffer, code->varDeclarationNumberOfResultTuples).copy());
    }

    // 2. copy watermark
    code->cleanupStmts.push_back(setWatermark(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 3. copy origin id
    code->cleanupStmts.push_back(setOriginId(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 4. copy sequence number
    code->cleanupStmts.push_back(setSequenceNumber(code->varDeclarationResultBuffer, code->varDeclarationInputBuffer).copy());

    // 5. emit the buffer to the runtime.
    code->cleanupStmts.push_back(
        emitTupleBuffer(code->varDeclarationExecutionContext, code->varDeclarationResultBuffer, code->varDeclarationWorkerContext)
            .copy());

    return true;
}

bool CCodeGenerator::generateCodeForWatermarkAssigner(Windowing::WatermarkStrategyPtr watermarkStrategy,
                                                      PipelineContextPtr context) {
    auto recordHandler = context->getRecordHandler();
    if (watermarkStrategy->getType() == Windowing::WatermarkStrategy::Type::EventTimeWatermark) {
        auto eventTimeWatermarkStrategy = watermarkStrategy->as<Windowing::EventTimeWatermarkStrategy>();

        auto tf = getTypeFactory();
        auto watermarkFieldName = eventTimeWatermarkStrategy->getField()->getFieldName();
        auto maxWatermarkVariableDeclaration =
            VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "maxWatermark");

        // initiate maxWatermark variable
        // auto maxWatermark = 0;
        auto maxWatermarkInitStatement = VarDeclStatement(maxWatermarkVariableDeclaration)
                                             .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(0_u64))));
        context->code->variableInitStmts.push_back(maxWatermarkInitStatement.copy());

        NES_ASSERT(!context->code->structDeclarationInputTuples.empty(), "invalid size of struct input tuples");
        // get the value for current watermark
        // uint64_t currentWatermark = record[index].ts;
        auto currentWatermarkVariableDeclaration =
            VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), "currentWatermark");

        auto getCreationTimestamp = call("getCreationTimestampInMS");
        auto tsVariableDeclaration = VarRef(context->code->varDeclarationInputBuffer).accessRef(getCreationTimestamp).copy();

        if (watermarkFieldName != Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME) {

            NES_ASSERT(recordHandler->hasAttribute(watermarkFieldName),
                       "CCodeGenerator: watermark assigner could not get field \"" << watermarkFieldName << "\" from struct");
            auto attribute = AttributeField::create(watermarkFieldName, DataTypeFactory::createUInt64());
            tsVariableDeclaration = recordHandler->getAttribute(attribute->getName());
        }

        auto calculateMaxTupleStatement = (*tsVariableDeclaration)
                * Constant(tf->createValueType(DataTypeFactory::createBasicValue(eventTimeWatermarkStrategy->getMultiplier())))
            - Constant(tf->createValueType(DataTypeFactory::createBasicValue(eventTimeWatermarkStrategy->getAllowedLateness())));
        auto currentWatermarkStatement = VarDeclStatement(currentWatermarkVariableDeclaration).assign(calculateMaxTupleStatement);
        context->code->currentCodeInsertionPoint->addStatement(currentWatermarkStatement.copy());

        /*
         * //we have to have this case to make sure that we don't get negative here when allowed lateness is larger that minwatermark
         * if(minWatermark < allowedLateness)
         *      minWatermark = 0
         */
        //  TODO check if needed
        //auto zero = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "0");
        // auto setWatermarkToZero = IF(VarRef(currentWatermarkVariableDeclaration) < Constant(tf->createValueType(
        //                                 DataTypeFactory::createBasicValue(eventTimeWatermarkStrategy->getAllowedLateness()))),
        //                             VarRef(currentWatermarkVariableDeclaration).assign(VarRef(zero)));
        //context->code->currentCodeInsertionPoint->addStatement(setWatermarkToZero.createCopy());

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
    } else if (watermarkStrategy->getType() == Windowing::WatermarkStrategy::Type::IngestionTimeWatermark) {
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
        //TODO currently we do not support allowed lateness for ingestion time windows
        setWatermarkFunctionCall.addParameter(VarRef(watermarkTsVariableDeclaration));
        auto setWatermarkStatement = VarRef(context->code->varDeclarationInputBuffer).accessRef(setWatermarkFunctionCall);

        context->code->currentCodeInsertionPoint->addStatement(setWatermarkStatement.createCopy());
    } else {
        NES_ERROR("CCodeGenerator: cannot generate code for watermark strategy");
    }

    return true;
}

void CCodeGenerator::generateCodeForWatermarkUpdaterWindow(const PipelineContextPtr& context,
                                                           const VariableDeclaration& handler) {
    auto updateAllWatermarkTsFunctionCall = FunctionCallStatement("updateMaxTs");
    updateAllWatermarkTsFunctionCall.addParameter(getWatermark(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(getOriginId(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(getSequenceNumber(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(VarRef(context->code->varDeclarationWorkerContext));
    auto updateAllWatermarkTsFunctionCallStatement = VarRef(handler).accessPtr(updateAllWatermarkTsFunctionCall);

    context->code->cleanupStmts.push_back(updateAllWatermarkTsFunctionCallStatement.createCopy());
}

void CCodeGenerator::generateCodeForWatermarkUpdaterJoin(const PipelineContextPtr& context,
                                                         const VariableDeclaration& handler,
                                                         bool leftSide) {
    auto updateAllWatermarkTsFunctionCall = FunctionCallStatement("updateMaxTs");
    updateAllWatermarkTsFunctionCall.addParameter(getWatermark(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(getOriginId(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(getSequenceNumber(context->code->varDeclarationInputBuffer));
    updateAllWatermarkTsFunctionCall.addParameter(VarRef(context->code->varDeclarationWorkerContext));
    auto tf = getTypeFactory();

    if (leftSide) {
        auto leftSideDecl = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "true");
        updateAllWatermarkTsFunctionCall.addParameter(VarRef(leftSideDecl));
    } else {
        auto leftSideDecl = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "false");
        updateAllWatermarkTsFunctionCall.addParameter(VarRef(leftSideDecl));
    }

    auto updateAllWatermarkTsFunctionCallStatement = VarRef(handler).accessPtr(updateAllWatermarkTsFunctionCall);

    context->code->cleanupStmts.push_back(updateAllWatermarkTsFunctionCallStatement.createCopy());
}

void CCodeGenerator::generateTupleBufferSpaceCheck(const PipelineContextPtr& context,
                                                   const VariableDeclaration& varDeclResultTuple,
                                                   const StructDeclaration& structDeclarationResultTuple,
                                                   SchemaPtr schema) {
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
        / Constant(tf->createValueType(DataTypeFactory::createBasicValue(resultTupleSize)));
    auto calculateMaxTupleAssignment = VarDeclStatement(maxTupleDeclaration).assign(calculateMaxTupleStatement);
    // 4. add statement to statement initialization (outside of loop to reduce multiple calculations) TODO: do we want to use the const keyword in generated code?
    code->variableInitStmts.push_back(calculateMaxTupleAssignment.copy());

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

    // 1.2 emit the output buffers to the Runtime -> pipelineExecutionContext.emitBuffer(resultTupleBuffer);
    thenStatement->addStatement(
        emitTupleBuffer(code->varDeclarationExecutionContext, code->varDeclarationResultBuffer, code->varDeclarationWorkerContext)
            .copy());
    // 2.1 reset the numberOfResultTuples to 0 -> numberOfResultTuples = 0;
    thenStatement->addStatement(
        VarRef(code->varDeclarationNumberOfResultTuples)
            .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(static_cast<uint64_t>(0)))))
            .copy());
    // 2.2 allocate a new buffer -> resultTupleBuffer = workerContext.allocateTupleBuffer();
    thenStatement->addStatement(
        VarRef(code->varDeclarationResultBuffer).assign(allocateTupleBuffer(code->varDeclarationWorkerContext)).copy());
    // 2.2 get typed result buffer from resultTupleBuffer -> resultTuples = (ResultTuple*)resultTupleBuffer.getBuffer();
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT) {
        thenStatement->addStatement(VarRef(varDeclResultTuple)
                                        .assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple))
                                        .copy());
    }

    // Setting the start of all fields for col layout
    if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT) {// TODO duplicate to maxTuple?
        std::vector<StatementPtr> statements;
        generateCodeInitStructFieldsColLayout(schema,
                                              tf,
                                              code->varDeclarationResultBuffer,
                                              structDeclarationResultTuple,
                                              varDeclResultTuple,
                                              statements,
                                              "capacityEmitThen");
        for (const auto& stmt : statements) {
            thenStatement->addStatement(stmt->createCopy());
        }
    }
}

std::vector<ExpressionStatementPtr> getKeyAssignmentExpressions(Windowing::LogicalWindowDefinitionPtr window,
                                                                std::shared_ptr<GeneratableTypesFactory>,
                                                                RecordHandlerPtr recordHandler) {
    std::vector<ExpressionStatementPtr> keyDeclarations;
    for (auto& key : window->getKeys()) {
        keyDeclarations.push_back(recordHandler->getAttribute(key->getFieldName()));

        //keyDeclarations.push_back(VariableDeclaration::create(tf->createDataType(key->getStamp()),key->getFieldName());
    }
    return keyDeclarations;
}

std::vector<VariableDeclaration> getKeyDeclarations(Windowing::LogicalWindowDefinitionPtr window,
                                                    std::shared_ptr<GeneratableTypesFactory> tf) {
    std::vector<VariableDeclaration> keyDeclarations;
    for (auto& key : window->getKeys()) {
        keyDeclarations.push_back(VariableDeclaration::create(tf->createDataType(key->getStamp()), key->getFieldName()));
    }
    return keyDeclarations;
}

StructDeclaration CCodeGenerator::generatePartialAggregationEntry(
    const Windowing::LogicalWindowDefinitionPtr window,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregation) {
    auto tf = getTypeFactory();
    auto keyVariableDeclaration = getKeyDeclarations(window, tf);
    // create struct for entry
    StructDeclaration structDeclarationTuple = StructDeclaration::create("PartialAggregationEntry", "partialAggregationEntry");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();
    for (auto decl : keyVariableDeclaration) {
        structDeclarationTuple.addField(decl);
    }
    auto aggregation = window->getWindowAggregation();
    for (auto& agg : generatableAggregation) {
        structDeclarationTuple.addField(*agg->getPartialAggregate());
    }
    return structDeclarationTuple;
}

/**
 * Code Generation for the window operator
 * @param window windowdefinition
 * @param context pipeline context
 * @param out
 * @return
 */
bool CCodeGenerator::generateCodeForThreadLocalPreAggregationOperator(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, aggregation);

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall =
        call("getOperatorHandler<Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    //auto workerId = (workerContext.getId()) % pipelineExecutionContext.getNumberOfWorkerThreads();
    auto workerId = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "workerId");
    auto callGetWorkerId = call("getId");
    auto getNumberOfWorkerThreads = call("getNumberOfWorkerThreads");
    auto workerIdStatement = VarDeclStatement(workerId).assign(
        VarRef(context->code->varDeclarationWorkerContext).accessRef(callGetWorkerId)
        % VarRef(context->code->varDeclarationExecutionContext).accessRef(getNumberOfWorkerThreads));
    context->code->variableInitStmts.push_back(workerIdStatement.copy());

    //auto& threadLocalSliceStore = windowOperatorHandler->threadLocalSliceStore[workerId];
    auto threadLocalState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "threadLocalState");
    auto getThreadLocalState = call("getThreadLocalSliceStore");
    getThreadLocalState->addParameter(VarRef(workerId));
    auto threadLocalStateStatement =
        VarDeclStatement(threadLocalState).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getThreadLocalState));
    context->code->variableInitStmts.push_back(threadLocalStateStatement.copy());

    // Read key value from record

    auto recordHandler = context->getRecordHandler();
    auto keyDeclarations = getKeyAssignmentExpressions(window, tf, recordHandler);

    auto timeCharacteristicField =
        Windowing::WindowType::asTimeBasedWindowType(window->getWindowType())->getTimeCharacteristic()->getField()->getName();
    auto getCreationTimestamp = call("getCreationTimestampInMS");
    auto tsVariableDeclaration = VarRef(context->code->varDeclarationInputBuffer).accessRef(getCreationTimestamp).copy();
    if (timeCharacteristicField != Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME) {
        tsVariableDeclaration = recordHandler->getAttribute(timeCharacteristicField);
    }

    //  auto& slice = threadLocalSliceStore.findSliceByTs(current_ts);
    auto slice = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "slice");
    auto findSliceByTs = call("findSliceByTs");
    findSliceByTs->addParameter(tsVariableDeclaration);
    auto sliceAssignmentStatement = VarDeclStatement(slice).assign(VarRef(threadLocalState).accessRef(findSliceByTs));
    context->code->currentCodeInsertionPoint->addStatement(sliceAssignmentStatement.copy());

    auto keyTuple = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "keyTuple");
    auto makeKeyTupleCall = call("std::make_tuple");
    // TODO this is not parable as it assumes a fix memory layout for tuple.
    for (int64_t i = keyDeclarations.size() - 1; i >= 0; i--) {
        makeKeyTupleCall->addParameter(keyDeclarations[i]);
    }

    context->code->currentCodeInsertionPoint->addStatement(VarDeclStatement(keyTuple).assign(makeKeyTupleCall).copy());

    auto getState = call("getState");
    auto hash = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "hash");
    auto calculateHashCall = call("calculateHash");
    calculateHashCall->addParameter(VarRef(keyTuple));
    auto hashCalculationStatement = VarDeclStatement(hash).assign(VarRef(slice).accessPtr(getState).accessRef(calculateHashCall));
    context->code->currentCodeInsertionPoint->addStatement(hashCalculationStatement.copy());

    // auto* entry = findOneEntry<K, useTags>(key, hash);
    auto entry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entry");
    auto findOneEntryCall = call("findOneEntry<>");
    findOneEntryCall->addParameter(VarRef(keyTuple));
    findOneEntryCall->addParameter(VarRef(hash));
    auto entryAssignmentStatement = VarDeclStatement(entry).assign(VarRef(slice).accessPtr(getState).accessRef(findOneEntryCall));
    context->code->currentCodeInsertionPoint->addStatement(entryAssignmentStatement.copy());

    auto ifElseStatement = IFELSE(!VarRef(entry));
    {
        auto entryNotSetCase = ifElseStatement.getTrueCaseCompoundStatement();
        // no entry for this key exists, so we create a new one
        auto insertEntry = call("insertEntry");
        insertEntry->addParameter(VarRef(hash));
        auto entryCreationStatement = VarRef(entry).assign(VarRef(slice).accessPtr(getState).accessRef(insertEntry));
        entryNotSetCase->addStatement(entryCreationStatement.copy());
        auto partialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "partialValue");
        auto getKeyPtrCall = call("getKeyPtr");
        getKeyPtrCall->addParameter(VarRef(entry));
        auto partialValueAssignment = VarDeclStatement(partialValue)
                                          .assign(TypeCast(VarRef(slice).accessPtr(getState).accessRef(getKeyPtrCall),
                                                           tf->createPointer(partialAggregationEntry.getType())));
        entryNotSetCase->addStatement(partialValueAssignment.copy());
        auto keyVariableDeclaration = getKeyDeclarations(window, tf);
        for (auto& key : window->getKeys()) {
            auto partialVariableDeclaration =
                VariableDeclaration::create(tf->createDataType(key->getStamp()), key->getFieldName());
            auto keyAssignmentStatement = VarRef(partialValue)
                                              .accessPtr(VarRef(partialVariableDeclaration))
                                              .assign(recordHandler->getAttribute(key->getFieldName()));
            entryNotSetCase->addStatement(keyAssignmentStatement.copy());
        }

        // set default partial values
        for (auto& agg : aggregation) {
            agg->compileLift(entryNotSetCase, VarRef(partialValue).accessPtr(VarRef(agg->getPartialAggregate())), recordHandler);
        }
    }

    {
        auto entryExistsCase = ifElseStatement.getFalseCaseCompoundStatement();

        // entry is valid case
        auto partialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "partialValue");
        auto getKeyPtrCall = call("getKeyPtr");
        getKeyPtrCall->addParameter(VarRef(entry));

        auto partialValueAssignment = VarDeclStatement(partialValue)
                                          .assign(TypeCast(VarRef(slice).accessPtr(getState).accessRef(getKeyPtrCall),
                                                           tf->createPointer(partialAggregationEntry.getType())));

        entryExistsCase->addStatement(partialValueAssignment.copy());
        for (auto& agg : aggregation) {
            agg->compileLiftCombine(entryExistsCase,
                                    VarRef(partialValue).accessPtr(VarRef(agg->getPartialAggregate())),
                                    recordHandler);
        }
    }
    context->code->currentCodeInsertionPoint->addStatement(ifElseStatement.createCopy());
    auto triggerThreadLocalStateCall = call("triggerThreadLocalState");
    triggerThreadLocalStateCall->addParameter(VarRef(context->code->varDeclarationWorkerContext));
    triggerThreadLocalStateCall->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    triggerThreadLocalStateCall->addParameter(VarRef(workerId));
    triggerThreadLocalStateCall->addParameter(getOriginId(context->code->varDeclarationInputBuffer));
    triggerThreadLocalStateCall->addParameter(getSequenceNumber(context->code->varDeclarationInputBuffer));
    triggerThreadLocalStateCall->addParameter(getWatermark(context->code->varDeclarationInputBuffer));

    auto triggerThreadLocalStateCallCallStatement =
        VarRef(windowOperatorHandlerDeclaration).accessPtr(triggerThreadLocalStateCall);

    context->code->cleanupStmts.push_back(triggerThreadLocalStateCallCallStatement.createCopy());

    return true;
}

/**
 * Code Generation for the window operator
 * @param window windowdefinition
 * @param context pipeline context
 * @param out
 * @return
 */
bool CCodeGenerator::generateCodeForNonKeyedThreadLocalPreAggregationOperator(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, aggregation);

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall =
        call("getOperatorHandler<Windowing::Experimental::NonKeyedThreadLocalPreAggregationOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    //auto workerId = (workerContext.getId()) % pipelineExecutionContext.getNumberOfWorkerThreads();
    auto workerId = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "workerId");
    auto callGetWorkerId = call("getId");
    auto getNumberOfWorkerThreads = call("getNumberOfWorkerThreads");
    auto workerIdStatement = VarDeclStatement(workerId).assign(
        VarRef(context->code->varDeclarationWorkerContext).accessRef(callGetWorkerId)
        % VarRef(context->code->varDeclarationExecutionContext).accessRef(getNumberOfWorkerThreads));
    context->code->variableInitStmts.push_back(workerIdStatement.copy());

    //auto& threadLocalSliceStore = windowOperatorHandler->threadLocalSliceStore[workerId];
    auto threadLocalState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "threadLocalState");
    auto getThreadLocalState = call("getThreadLocalSliceStore");
    getThreadLocalState->addParameter(VarRef(workerId));
    auto threadLocalStateStatement =
        VarDeclStatement(threadLocalState).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getThreadLocalState));
    context->code->variableInitStmts.push_back(threadLocalStateStatement.copy());

    // Read key value from record

    auto recordHandler = context->getRecordHandler();
    auto keyDeclarations = getKeyAssignmentExpressions(window, tf, recordHandler);

    auto timeCharacteristicField =
        Windowing::WindowType::asTimeBasedWindowType(window->getWindowType())->getTimeCharacteristic()->getField()->getName();
    auto getCreationTimestamp = call("getCreationTimestampInMS");
    auto tsVariableDeclaration = VarRef(context->code->varDeclarationInputBuffer).accessRef(getCreationTimestamp).copy();
    if (timeCharacteristicField != Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME) {
        tsVariableDeclaration = recordHandler->getAttribute(timeCharacteristicField);
    }

    //  auto& slice = threadLocalSliceStore.findSliceByTs(current_ts);
    auto slice = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "slice");
    auto findSliceByTs = call("findSliceByTs");
    findSliceByTs->addParameter(tsVariableDeclaration);
    auto sliceAssignmentStatement = VarDeclStatement(slice).assign(VarRef(threadLocalState).accessRef(findSliceByTs));
    context->code->currentCodeInsertionPoint->addStatement(sliceAssignmentStatement.copy());
    auto getState = call("getState");

    // auto* entry = findOneEntry<K, useTags>(key, hash);
    auto state = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "state");
    auto isInitialized = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "isInitialized");
    auto partialValuePtr = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "ptr");

    auto stateAssignmentStatement = VarDeclStatement(state).assign(VarRef(slice).accessPtr(getState));
    context->code->currentCodeInsertionPoint->addStatement(stateAssignmentStatement.copy());

    auto ifElseStatement = IFELSE(!VarRef(state).accessPtr(VarRef(isInitialized)));
    {
        auto stateNotInitializedCase = ifElseStatement.getTrueCaseCompoundStatement();
        auto partialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "partialValue");
        auto partialValueAssignment = VarDeclStatement(partialValue)
                                          .assign(TypeCast(VarRef(state).accessPtr(VarRef(partialValuePtr)),
                                                           tf->createPointer(partialAggregationEntry.getType())));
        stateNotInitializedCase->addStatement(partialValueAssignment.copy());

        // set default partial values
        for (auto& agg : aggregation) {
            agg->compileLift(stateNotInitializedCase,
                             VarRef(partialValue).accessPtr(VarRef(agg->getPartialAggregate())),
                             recordHandler);
        }
        auto initializeValue = VarRef(state).accessPtr(
            VarRef(isInitialized)
                .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(BasicType::BOOLEAN, "true")))));
        stateNotInitializedCase->addStatement(initializeValue.copy());
    }
    {
        auto stateInitializedCase = ifElseStatement.getFalseCaseCompoundStatement();

        // entry is valid case
        auto partialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "partialValue");
        auto partialValueAssignment = VarDeclStatement(partialValue)
                                          .assign(TypeCast(VarRef(state).accessPtr(VarRef(partialValuePtr)),
                                                           tf->createPointer(partialAggregationEntry.getType())));
        stateInitializedCase->addStatement(partialValueAssignment.copy());
        for (auto& agg : aggregation) {
            agg->compileLiftCombine(stateInitializedCase,
                                    VarRef(partialValue).accessPtr(VarRef(agg->getPartialAggregate())),
                                    recordHandler);
        }
    }
    context->code->currentCodeInsertionPoint->addStatement(ifElseStatement.createCopy());
    auto triggerThreadLocalStateCall = call("triggerThreadLocalState");
    triggerThreadLocalStateCall->addParameter(VarRef(context->code->varDeclarationWorkerContext));
    triggerThreadLocalStateCall->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    triggerThreadLocalStateCall->addParameter(VarRef(workerId));
    triggerThreadLocalStateCall->addParameter(getOriginId(context->code->varDeclarationInputBuffer));
    triggerThreadLocalStateCall->addParameter(getSequenceNumber(context->code->varDeclarationInputBuffer));
    triggerThreadLocalStateCall->addParameter(getWatermark(context->code->varDeclarationInputBuffer));

    auto triggerThreadLocalStateCallCallStatement =
        VarRef(windowOperatorHandlerDeclaration).accessPtr(triggerThreadLocalStateCall);

    context->code->cleanupStmts.push_back(triggerThreadLocalStateCallCallStatement.createCopy());

    return true;
}

bool CCodeGenerator::generateCodeForKeyedSliceMergingOperator(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto code = context->code;
    auto td = context->code;

    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, aggregation);
    context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);

    auto tupleBufferType = tf->createAnonymusDataType("NES::Runtime::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("Runtime::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer =
        VariableDeclaration::create(tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    VariableDeclaration varDeclarationWorkerContext =
        VariableDeclaration::create(tf->createReference(workerContextType), "workerContext");

    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationWorkerContext = varDeclarationWorkerContext;

    /* ExecutionResult ret = Ok; */
    // TODO probably it's not safe that we can mix enum values with int32 but it is a good hack for me :P
    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createAnonymusDataType("ExecutionResult"),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "ExecutionResult::Ok"))
            .copy());
    code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));

    code->returnStmt = ReturnStatement::create(VarRefStatement(*code->varDeclarationReturnValue).createCopy());

    auto mergeTask = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "mergeTask");
    code->variableInitStmts.push_back(
        VarDeclStatement(mergeTask)
            .assign(TypeCast(getBuffer(code->varDeclarationInputBuffer),
                             tf->createAnonymusDataType("NES::Windowing::Experimental::SliceMergeTask*")))
            .copy());
    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::KeyedSliceMergingOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
                                       .assign(VarRef(code->varDeclarationExecutionContext).accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    // auto partitions = windowOperatorHandler->getSliceStaging().erasePartition(mergeTask->sliceEnd);
    auto sliceIndex = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "endSlice");
    auto sliceStart = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "startSlice");
    auto getSliceStagingCall = call("getSliceStaging");
    auto erasePartitionCall = call("erasePartition");
    erasePartitionCall->addParameter(VarRef(mergeTask).accessPtr(VarRef(sliceIndex)));
    auto partitions = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "partitions");
    auto partitionStatement =
        VarDeclStatement(partitions)
            .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getSliceStagingCall).accessRef(erasePartitionCall));
    context->code->variableInitStmts.push_back(partitionStatement.copy());

    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");
    auto createSliceCall = call("createKeyedSlice");
    createSliceCall->addParameter(VarRef(mergeTask));
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSlice).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(createSliceCall)).copy());

    auto globalSliceState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "globalSliceState");
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSliceState).assign(VarRef(globalSlice).accessPtr(call("getState"))).copy());
    auto buffers = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffers");
    auto partitionBuffers = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "partitionBuffers");
    context->code->variableInitStmts.push_back(
        VarDeclStatement(partitionBuffers).assign(VarRef(partitions).accessPtr(VarRef(buffers))).copy());
    // for (auto& partition : (*partitions.get())) {
    context->code->variableInitStmts.push_back(keyedSliceMergeLoop(partitionBuffers,
                                                                   code->tupleBufferGetNumberOfTupleCall,
                                                                   partialAggregationEntry,
                                                                   window,
                                                                   aggregation,
                                                                   context));

    return 0;
}

bool CCodeGenerator::generateCodeForNonKeyedSliceMergingOperator(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto code = context->code;
    auto td = context->code;

    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, aggregation);
    context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);

    auto tupleBufferType = tf->createAnonymusDataType("NES::Runtime::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("Runtime::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer =
        VariableDeclaration::create(tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    VariableDeclaration varDeclarationWorkerContext =
        VariableDeclaration::create(tf->createReference(workerContextType), "workerContext");

    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationWorkerContext = varDeclarationWorkerContext;

    /* ExecutionResult ret = Ok; */
    // TODO probably it's not safe that we can mix enum values with int32 but it is a good hack for me :P
    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createAnonymusDataType("ExecutionResult"),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "ExecutionResult::Ok"))
            .copy());
    code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));

    code->returnStmt = ReturnStatement::create(VarRefStatement(*code->varDeclarationReturnValue).createCopy());

    auto mergeTask = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "mergeTask");
    code->variableInitStmts.push_back(
        VarDeclStatement(mergeTask)
            .assign(TypeCast(getBuffer(code->varDeclarationInputBuffer),
                             tf->createAnonymusDataType("NES::Windowing::Experimental::SliceMergeTask*")))
            .copy());
    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::NonKeyedSliceMergingOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
                                       .assign(VarRef(code->varDeclarationExecutionContext).accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    // auto partitions = windowOperatorHandler->getSliceStaging().erasePartition(mergeTask->sliceEnd);
    auto sliceIndex = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "endSlice");
    auto sliceStart = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "startSlice");
    auto getSliceStagingCall = call("getSliceStaging");
    auto erasePartitionCall = call("erasePartition");
    erasePartitionCall->addParameter(VarRef(mergeTask).accessPtr(VarRef(sliceIndex)));
    auto partitions = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "partitions");
    auto partitionStatement =
        VarDeclStatement(partitions)
            .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getSliceStagingCall).accessRef(erasePartitionCall));
    context->code->variableInitStmts.push_back(partitionStatement.copy());

    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");
    auto createSliceCall = call("createGlobalSlice");
    createSliceCall->addParameter(VarRef(mergeTask));
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSlice).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(createSliceCall)).copy());

    auto globalSliceState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "globalSliceState");
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSliceState).assign(VarRef(globalSlice).accessPtr(call("getState"))).copy());
    auto partialStates = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialStates");
    context->code->variableInitStmts.push_back(
        VarDeclStatement(partialStates).assign(VarRef(partitions).accessPtr(VarRef(partialStates))).copy());

    // for (auto& partition : (*partitions.get())) {
    context->code->variableInitStmts.push_back(globalSliceMergeLoop(partialStates,
                                                                    code->tupleBufferGetNumberOfTupleCall,
                                                                    partialAggregationEntry,
                                                                    window,
                                                                    aggregation,
                                                                    context));

    return 0;
}

bool CCodeGenerator::generateCodeForKeyedSliceStoreAppend(PipelineContextPtr context, uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto code = context->code;
    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");
    auto partitions = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "partitions");
    auto sliceIndex = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "sliceIndex");

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler2");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
                                       .assign(VarRef(code->varDeclarationExecutionContext).accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());
    auto triggerSliceMergingCall = call("triggerSliceMerging");
    triggerSliceMergingCall->addParameter(VarRef(code->varDeclarationWorkerContext));
    triggerSliceMergingCall->addParameter(VarRef(code->varDeclarationExecutionContext));
    triggerSliceMergingCall->addParameter(VarRef(partitions).accessPtr(VarRef(sliceIndex)));
    auto moveCall = call("std::move");
    moveCall->addParameter(VarRef(globalSlice));
    triggerSliceMergingCall->addParameter(moveCall);
    auto triggerSliceStatement = VarRefStatement(windowOperatorHandlerDeclaration).accessPtr(triggerSliceMergingCall);
    context->code->variableInitStmts.push_back(triggerSliceStatement.copy());
    return true;
}

bool CCodeGenerator::generateCodeForNonKeyedSliceStoreAppend(PipelineContextPtr context, uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto code = context->code;
    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");
    auto partitions = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "partitions");
    auto sliceIndex = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "sliceIndex");

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler2");
    auto getOperatorHandlerCall =
        call("getOperatorHandler<Windowing::Experimental::NonKeyedGlobalSliceStoreAppendOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
                                       .assign(VarRef(code->varDeclarationExecutionContext).accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());
    auto triggerSliceMergingCall = call("triggerSliceMerging");
    triggerSliceMergingCall->addParameter(VarRef(code->varDeclarationWorkerContext));
    triggerSliceMergingCall->addParameter(VarRef(code->varDeclarationExecutionContext));
    triggerSliceMergingCall->addParameter(VarRef(partitions).accessPtr(VarRef(sliceIndex)));
    auto moveCall = call("std::move");
    moveCall->addParameter(VarRef(globalSlice));
    triggerSliceMergingCall->addParameter(moveCall);
    auto triggerSliceStatement = VarRefStatement(windowOperatorHandlerDeclaration).accessPtr(triggerSliceMergingCall);
    context->code->variableInitStmts.push_back(triggerSliceStatement.copy());
    return true;
}

uint64_t getAggregationValueSize(Windowing::LogicalWindowDefinitionPtr window) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    uint64_t aggSize = 0;
    for (auto agg : window->getWindowAggregation()) {
        if (agg->getType() == Windowing::WindowAggregationDescriptor::Type::Avg) {
            // a avg is currently a custome type
            aggSize = aggSize + 16;
        } else if (agg->getType() == Windowing::WindowAggregationDescriptor::Type::Median) {
            // a avg is currently a custome type
            aggSize = aggSize + 12;
        } else {
            aggSize = aggSize + physicalDataTypeFactory.getPhysicalType(agg->getPartialAggregateStamp())->size();
        }
    }
    return aggSize;
}

uint64_t getKeySpaceSize(Windowing::LogicalWindowDefinitionPtr window) {
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    uint64_t keysSize = 0;
    for (auto key : window->getKeys()) {
        keysSize = keysSize + physicalDataTypeFactory.getPhysicalType(key->getStamp())->size();
    }
    return keysSize;
}

uint64_t getEntrySize(Windowing::LogicalWindowDefinitionPtr window) {
    uint64_t keysSize = getKeySpaceSize(window);
    uint64_t valueSize = getAggregationValueSize(window);
    uint64_t entryHeaderSize = 16;
    uint64_t entrySize = entryHeaderSize + keysSize + valueSize;
    return entrySize;
}

std::shared_ptr<ForLoopStatement> CCodeGenerator::keyedSliceMergeLoop(
    VariableDeclaration& buffers,
    FunctionCallStatement& tupleBufferGetNumberOfTupleCall,
    StructDeclaration& partialAggregationEntry,
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr) {
    auto tf = getTypeFactory();
    auto getSizeCall = call("size");
    auto globalSliceState = VariableDeclaration ::create(tf->createAnonymusDataType("auto&"), "globalSliceState");

    auto partitionIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "partitionIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());
    auto partitionLoop = std::make_shared<FOR>(partitionIndex,
                                               (VarRef(partitionIndex) < (VarRef(buffers).accessRef(getSizeCall))).copy(),
                                               (++VarRef(partitionIndex)).copy());
    {
        auto loopBody = partitionLoop->getCompoundStatement();
        auto buffer = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffer");
        auto entries = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entries");
        // buffer = partitions[partitionIndex];
        loopBody->addStatement(VarDeclStatement(buffer).assign(VarRef(buffers)[VarRef(partitionIndex)]).copy());
        loopBody->addStatement(VarDeclStatement(entries).assign(VarRef(buffer).accessRef(call("getBuffer"))).copy());
        // scan over buffer
        auto recordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
            VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                        "recordIndex",
                                        DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
                .copy());

        auto keyVariableDeclarations = getKeyDeclarations(window, tf);

        auto scanLoop =
            std::make_shared<FOR>(recordIndex,
                                  (VarRef(recordIndex) < (VarRef(buffer).accessRef(tupleBufferGetNumberOfTupleCall))).copy(),
                                  (++VarRef(recordIndex)).copy());
        {
            auto scanBody = scanLoop->getCompoundStatement();
            auto entry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entry");
            // todo fix entry size
            uint64_t entryHeaderSize = 16;
            auto entrySizeConst = Constant(tf->createValueType(DataTypeFactory::createBasicValue(getEntrySize(window))));
            auto entryOffset = Constant(tf->createValueType(DataTypeFactory::createBasicValue(entryHeaderSize)));
            auto entryAssignmentStatement =
                VarDeclStatement(entry).assign(TypeCast(VarRef(entries) + ((entrySizeConst * VarRef(recordIndex))) + entryOffset,
                                                        tf->createPointer(partialAggregationEntry.getType())));
            scanBody->addStatement(entryAssignmentStatement.copy());
            auto keyDeclarations = getKeyDeclarations(window, tf);
            auto globalEntry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "globalEntry");

            auto tuple = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "tuple");
            auto makeTuple = call("std::make_tuple");
            // TODO this is not parable as it assumes a fix memory layout for tuple.
            for (int64_t i = keyDeclarations.size() - 1; i >= 0; i--) {
                makeTuple->addParameter(VarRef(entry).accessPtr(VarRef(keyDeclarations[i])));
            }
            //for (auto& keyDeclaration : keyDeclarations) {
            //     makeTuple->addParameter(VarRef(entry).accessPtr(VarRef(keyDeclaration)));
            //}
            scanBody->addStatement(VarDeclStatement(tuple).assign(makeTuple).copy());

            auto hash = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "hash");
            auto calculateHashCall = call("calculateHash");
            calculateHashCall->addParameter(VarRef(tuple));
            auto hashCalculationStatement = VarDeclStatement(hash).assign(VarRef(globalSliceState).accessRef(calculateHashCall));
            scanBody->addStatement(hashCalculationStatement.copy());

            // auto* entry = findOneEntry<K, useTags>(key, hash);
            auto findOneEntryCall = call("findOneEntry<>");
            findOneEntryCall->addParameter(VarRef(tuple));
            findOneEntryCall->addParameter(VarRef(hash));
            auto globalEntryAssignmentStatement =
                VarDeclStatement(globalEntry).assign(VarRef(globalSliceState).accessRef(findOneEntryCall));

            // auto getEntry = call("getEntry<>");
            // getEntry->addParameter(VarRef(tuple));
            //   auto globalEntryAssignmentStatement = VarDeclStatement(globalEntry)
            //          .assign(TypeCast(VarRef(globalSliceState).accessRef(getEntry),
            //                          tf->createPointer(partialAggregationEntry.getType())));
            scanBody->addStatement(globalEntryAssignmentStatement.copy());

            auto ifElseStatement = IFELSE(!VarRef(globalEntry));
            {
                auto entryNotSetCase = ifElseStatement.getTrueCaseCompoundStatement();
                // no entry for this key exists, so we create a new one
                auto insertEntry = call("insertEntry");
                insertEntry->addParameter(VarRef(hash));
                auto entryCreationStatement = VarRef(globalEntry).assign(VarRef(globalSliceState).accessRef(insertEntry));
                entryNotSetCase->addStatement(entryCreationStatement.copy());
                auto partialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "partialValue");
                auto getKeyPtrCall = call("getKeyPtr");
                getKeyPtrCall->addParameter(VarRef(globalEntry));
                auto partialValueAssignment = VarDeclStatement(partialValue)
                                                  .assign(TypeCast(VarRef(globalSliceState).accessRef(getKeyPtrCall),
                                                                   tf->createPointer(partialAggregationEntry.getType())));
                entryNotSetCase->addStatement(partialValueAssignment.copy());

                auto memcopyCall = call("std::memcpy");
                memcopyCall->addParameter(VarRef(partialValue));
                memcopyCall->addParameter(VarRef(entry));
                uint64_t entryContentSize = getKeySpaceSize(window) + getAggregationValueSize(window);
                memcopyCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(entryContentSize))));
                entryNotSetCase->addStatement(memcopyCall);
            }

            {
                auto entryExistsCase = ifElseStatement.getFalseCaseCompoundStatement();

                // entry is valid case
                auto partialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "partialValue");
                auto getKeyPtrCall = call("getKeyPtr");
                getKeyPtrCall->addParameter(VarRef(globalEntry));

                auto partialValueAssignment = VarDeclStatement(partialValue)
                                                  .assign(TypeCast(VarRef(globalSliceState).accessRef(getKeyPtrCall),
                                                                   tf->createPointer(partialAggregationEntry.getType())));

                entryExistsCase->addStatement(partialValueAssignment.copy());
                for (auto& agg : aggregation) {
                    agg->compileCombine(entryExistsCase, VarRef(partialValue), VarRef(entry));
                }
            }
            scanBody->addStatement(ifElseStatement.createCopy());
        }
        loopBody->addStatement(scanLoop);
    }
    return partitionLoop;
}

std::shared_ptr<ForLoopStatement> CCodeGenerator::globalSliceMergeLoop(
    VariableDeclaration& buffers,
    FunctionCallStatement&,
    StructDeclaration& partialAggregationEntry,
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr) {
    auto tf = getTypeFactory();
    auto getSizeCall = call("size");
    auto entryPtr = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "ptr");
    auto globalSliceState = VariableDeclaration ::create(tf->createAnonymusDataType("auto&"), "globalSliceState");
    auto globalPartialEntry = VariableDeclaration ::create(tf->createAnonymusDataType("auto*"), "globalPartialEntry");

    auto partitionIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "partitionIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());
    auto partitionLoop = std::make_shared<FOR>(partitionIndex,
                                               (VarRef(partitionIndex) < (VarRef(buffers).accessRef(getSizeCall))).copy(),
                                               (++VarRef(partitionIndex)).copy());
    {
        auto loopBody = partitionLoop->getCompoundStatement();
        auto partialValueAssignment = VarDeclStatement(globalPartialEntry)
                                          .assign(TypeCast(VarRef(globalSliceState).accessPtr(VarRef(entryPtr)),
                                                           tf->createPointer(partialAggregationEntry.getType())));
        loopBody->addStatement(partialValueAssignment.createCopy());

        auto state = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "state");
        // buffer = partitions[partitionIndex];
        loopBody->addStatement(VarDeclStatement(state).assign(VarRef(buffers)[VarRef(partitionIndex)]).copy());

        auto localEntry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "localEntry");

        auto localPartialValueAssignment = VarDeclStatement(localEntry)
                                               .assign(TypeCast(VarRef(state).accessPtr(VarRef(entryPtr)),
                                                                tf->createPointer(partialAggregationEntry.getType())));
        loopBody->addStatement(localPartialValueAssignment.copy());
        auto isInitialized = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "isInitialized");

        auto ifElseStatement = IFELSE(!VarRef(globalSliceState).accessPtr(VarRef(isInitialized)));
        {
            auto stateNotInitializedCase = ifElseStatement.getTrueCaseCompoundStatement();
            auto memcopyCall = call("std::memcpy");
            memcopyCall->addParameter(VarRef(globalPartialEntry));
            memcopyCall->addParameter(VarRef(localEntry));
            uint64_t entryContentSize = getAggregationValueSize(window);
            memcopyCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(entryContentSize))));
            stateNotInitializedCase->addStatement(memcopyCall);
            auto initializeValue = VarRef(state).accessPtr(
                VarRef(isInitialized)
                    .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(BasicType::BOOLEAN, "true")))));
            stateNotInitializedCase->addStatement(initializeValue.copy());
        }

        {
            auto entryExistsCase = ifElseStatement.getFalseCaseCompoundStatement();
            for (auto& agg : aggregation) {
                agg->compileCombine(entryExistsCase, VarRef(globalPartialEntry), VarRef(localEntry));
            }
        }

        loopBody->addStatement(ifElseStatement.createCopy());
    }
    return partitionLoop;
}

ExpressionStatementPtr CCodeGenerator::createGetEntryCall(Windowing::LogicalWindowDefinitionPtr window,
                                                          PipelineContextPtr context) {
    auto tf = getTypeFactory();
    auto recordHandler = context->getRecordHandler();
    auto keyDeclarations = getKeyAssignmentExpressions(window, tf, recordHandler);
    auto tuple = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "tuple");
    auto makeTuple = call("std::make_tuple");
    // TODO this is not parable as it assumes a fix memory layout for tuple.
    for (int64_t i = keyDeclarations.size() - 1; i >= 0; i--) {
        makeTuple->addParameter(keyDeclarations[i]);
    }
    for (auto& keyDeclaration : keyDeclarations) {
        makeTuple->addParameter(keyDeclaration);
    }
    context->code->currentCodeInsertionPoint->addStatement(VarDeclStatement(tuple).assign(makeTuple).copy());
    auto getEntry = call("getEntry<>");
    getEntry->addParameter(VarRef(tuple));
    return getEntry;
}

bool CCodeGenerator::generateCodeForNonKeyedSlidingWindowSink(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex,
    SchemaPtr resultSchema) {
    auto tf = getTypeFactory();
    auto code = context->code;
    auto td = context->code;
    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, aggregation);
    context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);
    //auto keyStamp = window->isKeyed() ? window->getOnKey()[0]->getStamp() : window->getWindowAggregation()[0]->on()->getStamp();
    //auto physicalKeyType = tf->createDataType(keyStamp);
    auto keyVariableDeclaration = getKeyDeclarations(window, tf);
    //auto keyVariableDeclaration = partialAggregationEntry.getVariableDeclaration(window->getOnKey()[0]->getFieldName());

    auto tupleBufferType = tf->createAnonymusDataType("NES::Runtime::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("Runtime::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer =
        VariableDeclaration::create(tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    VariableDeclaration varDeclarationWorkerContext =
        VariableDeclaration::create(tf->createReference(workerContextType), "workerContext");

    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationWorkerContext = varDeclarationWorkerContext;

    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createAnonymusDataType("ExecutionResult"),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "ExecutionResult::Ok"))
            .copy());
    code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));

    code->returnStmt = ReturnStatement::create(VarRefStatement(*code->varDeclarationReturnValue).createCopy());

    auto mergeTask = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "mergeTask");
    code->variableInitStmts.push_back(
        VarDeclStatement(mergeTask)
            .assign(TypeCast(getBuffer(code->varDeclarationInputBuffer),
                             tf->createAnonymusDataType("NES::Windowing::Experimental::WindowTriggerTask*")))
            .copy());
    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
                                       .assign(VarRef(code->varDeclarationExecutionContext).accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    // auto startSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "startSlice");
    auto endSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "endSlice");

    //auto assignment = VarDeclStatement(startSlice).assign(VarRef(mergeTask).accessPtr(VarRef(startSlice)));
    // context->code->variableInitStmts.push_back(assignment.copy());

    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");
    auto createSliceCall = call("createGlobalSlice");
    createSliceCall->addParameter(VarRef(mergeTask));
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSlice).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(createSliceCall)).copy());

    auto globalSliceState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "globalSliceState");
    auto sliceStatePtr = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "ptr");
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSliceState).assign(VarRef(globalSlice).accessPtr(call("getState"))).copy());

    auto windowSlices = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowSlices");
    auto getSlicesForWindowCall = call("getSlicesForWindow");
    getSlicesForWindowCall->addParameter(VarRef(mergeTask));
    context->code->variableInitStmts.push_back(
        VarDeclStatement(windowSlices).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getSlicesForWindowCall)).copy());

    auto sliceIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "sliceIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());

    auto globalSliceLoop = std::make_shared<FOR>(sliceIndex,
                                                 (VarRef(sliceIndex) < (VarRef(windowSlices).accessRef(call("size")))).copy(),
                                                 (++VarRef(sliceIndex)).copy());
    {
        auto body = globalSliceLoop->getCompoundStatement();
        auto getStateCall = call("getState");

        auto buffers = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffers");
        auto isInitialized = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "isInitialized");
        body->addStatement(
            VarDeclStatement(buffers)
                .assign(VarRef(windowSlices)[VarRef(sliceIndex)].accessPtr(getStateCall).accessPtr(VarRef(sliceStatePtr)))
                .copy());

        auto ifElseStatement = IFELSE(!VarRef(globalSliceState).accessPtr(VarRef(isInitialized)));
        {
            auto stateNotInitializedCase = ifElseStatement.getTrueCaseCompoundStatement();
            auto memcopyCall = call("std::memcpy");
            memcopyCall->addParameter(VarRef(globalSliceState).accessPtr(VarRef(sliceStatePtr)));
            memcopyCall->addParameter(VarRef(buffers));
            uint64_t entryContentSize = getAggregationValueSize(window);
            memcopyCall->addParameter(Constant(tf->createValueType(DataTypeFactory::createBasicValue(entryContentSize))));
            stateNotInitializedCase->addStatement(memcopyCall);
            auto initializeValue = VarRef(globalSliceState)
                                       .accessPtr(VarRef(isInitialized)
                                                      .assign(Constant(tf->createValueType(
                                                          DataTypeFactory::createBasicValue(BasicType::BOOLEAN, "true")))));
            stateNotInitializedCase->addStatement(initializeValue.copy());
        }
        {
            auto entryExistsCase = ifElseStatement.getFalseCaseCompoundStatement();
            auto localPartialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "localPartialValue");
            auto localPartialValueAssignment =
                VarDeclStatement(localPartialValue)
                    .assign(TypeCast(VarRef(buffers), tf->createPointer(partialAggregationEntry.getType())));
            entryExistsCase->addStatement(localPartialValueAssignment.createCopy());
            auto globalPartialValue = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalPartialValue");
            auto globalPartialValueAssignment = VarDeclStatement(globalPartialValue)
                                                    .assign(TypeCast(VarRef(globalSliceState).accessPtr(VarRef(sliceStatePtr)),
                                                                     tf->createPointer(partialAggregationEntry.getType())));
            entryExistsCase->addStatement(globalPartialValueAssignment.createCopy());
            for (auto& agg : aggregation) {
                agg->compileCombine(entryExistsCase, VarRef(globalPartialValue), VarRef(localPartialValue));
            }
        }

        body->addStatement(ifElseStatement.createCopy());
    }

    context->code->variableInitStmts.push_back(globalSliceLoop);
    // TODO merge with global tumbling window sink

    auto entryPtr = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "ptr");

    auto entry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entry");
    auto blockScope = BlockScopeStatement::create();
    auto entryAssignmentStatement = VarDeclStatement(entry).assign(
        TypeCast(VarRef(globalSliceState).accessPtr(VarRef(entryPtr)), tf->createPointer(partialAggregationEntry.getType())));

    blockScope->addStatement(entryAssignmentStatement.copy());
    // todo currently we assume here that the output schema always has the following fields in the same order.
    // assign start field
    context->getRecordHandler()->registerAttribute(resultSchema->fields[0]->getName(),
                                                   VarRef(globalSlice).accessPtr(call("getStart")).copy());
    // assign end field
    context->getRecordHandler()->registerAttribute(resultSchema->fields[1]->getName(),
                                                   VarRef(globalSlice).accessPtr(call("getEnd")).copy());

    // assign value field
    for (auto& agg : aggregation) {
        auto value = agg->getPartialAggregate();
        auto finalAggValue = agg->lower(VarRef(entry).accessPtr(VarRef(value)).copy());
        auto asFieldFieldName = agg->getAggregationDescriptor()->as()->as<FieldAccessExpressionNode>()->getFieldName();
        context->getRecordHandler()->registerAttribute(asFieldFieldName, finalAggValue);
    }

    code->currentCodeInsertionPoint = blockScope;

    context->code->cleanupStmts.push_back(blockScope);

    return 0;
}

bool CCodeGenerator::generateCodeForKeyedSlidingWindowSink(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex,
    SchemaPtr resultSchema) {
    auto tf = getTypeFactory();
    auto code = context->code;
    auto td = context->code;
    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, aggregation);
    context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);
    //auto keyStamp = window->isKeyed() ? window->getOnKey()[0]->getStamp() : window->getWindowAggregation()[0]->on()->getStamp();
    //auto physicalKeyType = tf->createDataType(keyStamp);
    auto keyVariableDeclaration = getKeyDeclarations(window, tf);
    //auto keyVariableDeclaration = partialAggregationEntry.getVariableDeclaration(window->getOnKey()[0]->getFieldName());

    auto tupleBufferType = tf->createAnonymusDataType("NES::Runtime::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("Runtime::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer =
        VariableDeclaration::create(tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    VariableDeclaration varDeclarationWorkerContext =
        VariableDeclaration::create(tf->createReference(workerContextType), "workerContext");

    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationWorkerContext = varDeclarationWorkerContext;

    /* ExecutionResult ret = Ok; */
    // TODO probably it's not safe that we can mix enum values with int32 but it is a good hack for me :P
    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createAnonymusDataType("ExecutionResult"),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "ExecutionResult::Ok"))
            .copy());
    code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));

    code->returnStmt = ReturnStatement::create(VarRefStatement(*code->varDeclarationReturnValue).createCopy());

    auto mergeTask = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "mergeTask");
    code->variableInitStmts.push_back(
        VarDeclStatement(mergeTask)
            .assign(TypeCast(getBuffer(code->varDeclarationInputBuffer),
                             tf->createAnonymusDataType("NES::Windowing::Experimental::WindowTriggerTask*")))
            .copy());
    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto windowOperatorStatement = VarDeclStatement(windowOperatorHandlerDeclaration)
                                       .assign(VarRef(code->varDeclarationExecutionContext).accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(windowOperatorStatement.copy());

    // auto startSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "startSlice");
    auto endSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "endSlice");

    //auto assignment = VarDeclStatement(startSlice).assign(VarRef(mergeTask).accessPtr(VarRef(startSlice)));
    // context->code->variableInitStmts.push_back(assignment.copy());

    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");
    auto createSliceCall = call("createKeyedSlice");
    createSliceCall->addParameter(VarRef(mergeTask));
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSlice).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(createSliceCall)).copy());

    auto globalSliceState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "globalSliceState");
    context->code->variableInitStmts.push_back(
        VarDeclStatement(globalSliceState).assign(VarRef(globalSlice).accessPtr(call("getState"))).copy());

    auto windowSlices = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowSlices");
    auto getSlicesForWindowCall = call("getSlicesForWindow");
    getSlicesForWindowCall->addParameter(VarRef(mergeTask));
    context->code->variableInitStmts.push_back(
        VarDeclStatement(windowSlices).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getSlicesForWindowCall)).copy());

    auto sliceIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "sliceIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());

    auto globalSliceLoop = std::make_shared<FOR>(sliceIndex,
                                                 (VarRef(sliceIndex) < (VarRef(windowSlices).accessRef(call("size")))).copy(),
                                                 (++VarRef(sliceIndex)).copy());
    {
        auto body = globalSliceLoop->getCompoundStatement();

        // buffers = (*windowOperatorHandler->getGlobalSliceStore().getSlice(0)->getState().getEntries().get())
        //auto getGlobalSliceStoreCall = call("getGlobalSliceStore");
        //auto hasSliceCall = call("hasSlice");
        //hasSliceCall->addParameter(VarRef(startSlice));

        // if(!windowOperatorHandler->getGlobalSliceStore().hasSlice(0)) continue;
        //auto ifStm = IF(!VarRef(windowOperatorHandlerDeclaration).accessPtr(getGlobalSliceStoreCall).accessRef(hasSliceCall));
        //ifStm.getCompoundStatement()->addStatement(Continue().createCopy());
        //body->addStatement(ifStm.createCopy());

        //auto getSliceCall = call("getSlice");
        //getSliceCall->addParameter(VarRef(startSlice));
        auto getStateCall = call("getState");
        auto getEntriesCall = call("getEntries");
        auto getCall = call("get");
        auto pointerRefCall = call("*");
        pointerRefCall->addParameter(
            VarRef(windowSlices)[VarRef(sliceIndex)].accessPtr(getStateCall).accessRef(getEntriesCall).accessRef(getCall));

        auto buffers = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffers");
        body->addStatement(VarDeclStatement(buffers).assign(pointerRefCall).copy());
        body->addStatement(keyedSliceMergeLoop(buffers,
                                               code->tupleBufferGetNumberOfTupleCall,
                                               partialAggregationEntry,
                                               window,
                                               aggregation,
                                               context));
    }

    context->code->variableInitStmts.push_back(globalSliceLoop);
    // TODO merge with tumbling window sink

    auto buffer = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffer");
    auto entries = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entries");
    auto buffers = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffers");

    auto buffersAssignment = VarDeclStatement(buffers).assign(VarRef(globalSliceState).accessRef(call("extractEntries")));
    context->code->variableInitStmts.push_back(buffersAssignment.copy());
    // for (auto& partition : (*partitions.get())) {
    auto getSizeCall = call("size");

    auto partitionIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "partitionIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());
    auto partitionLoop = std::make_shared<FOR>(partitionIndex,
                                               (VarRef(partitionIndex) < (VarRef(buffers).accessPtr(getSizeCall))).copy(),
                                               (++VarRef(partitionIndex)).copy());
    {
        auto loopBody = partitionLoop->getCompoundStatement();
        auto buffer = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffer");
        auto entries = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entries");
        // buffer = partitions[partitionIndex];
        auto getCall = call("operator[]");
        getCall->addParameter(VarRef(partitionIndex));
        loopBody->addStatement(VarDeclStatement(buffer).assign(VarRef(buffers).accessPtr(getCall)).copy());
        loopBody->addStatement(VarDeclStatement(entries).assign(VarRef(buffer).accessRef(call("getBuffer"))).copy());
        // scan over buffer
        auto recordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
            VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                        "recordIndex",
                                        DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
                .copy());

        auto scanLoop = std::make_shared<FOR>(
            recordIndex,
            (VarRef(recordIndex) < (VarRef(buffer).accessRef(context->code->tupleBufferGetNumberOfTupleCall))).copy(),
            (++VarRef(recordIndex)).copy());
        {
            auto scanBody = scanLoop->getCompoundStatement();
            auto entry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entry");
            // todo fix entry size
            auto entrySize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(getEntrySize(window))));
            auto entryOffset = Constant(tf->createValueType(DataTypeFactory::createBasicValue(16_u64)));
            auto entryAssignmentStatement =
                VarDeclStatement(entry).assign(TypeCast(VarRef(entries) + ((entrySize * VarRef(recordIndex))) + entryOffset,
                                                        tf->createPointer(partialAggregationEntry.getType())));

            scanBody->addStatement(entryAssignmentStatement.copy());
            // todo currently we assume here that the output schema always has the following fields in the same order.
            // assign start field
            context->getRecordHandler()->registerAttribute(resultSchema->fields[0]->getName(),
                                                           VarRef(globalSlice).accessPtr(call("getStart")).copy());
            // assign end field
            context->getRecordHandler()->registerAttribute(resultSchema->fields[1]->getName(),
                                                           VarRef(globalSlice).accessPtr(call("getEnd")).copy());
            // assign key field
            auto keyDeclarations = getKeyDeclarations(window, tf);
            for (auto key : keyDeclarations) {
                //auto key = partialAggregationEntry.getVariableDeclaration(window->getOnKey()[0]->getFieldName());
                context->getRecordHandler()->registerAttribute(key.getIdentifierName(),
                                                               VarRef(entry).accessPtr(VarRef(key)).copy());
            }
            // assign value field
            // assign value field
            for (auto& agg : aggregation) {
                auto value = agg->getPartialAggregate();
                auto finalAggValue = agg->lower(VarRef(entry).accessPtr(VarRef(value)).copy());
                auto asFieldFieldName = agg->getAggregationDescriptor()->as()->as<FieldAccessExpressionNode>()->getFieldName();
                context->getRecordHandler()->registerAttribute(asFieldFieldName, finalAggValue);
            }

            context->code->currentCodeInsertionPoint = scanBody;
        }
        loopBody->addStatement(scanLoop);
    }
    context->code->cleanupStmts.push_back(partitionLoop);

    return 0;
}

bool CCodeGenerator::generateCodeForKeyedTumblingWindowSink(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    SchemaPtr resultSchema) {
    auto tf = getTypeFactory();
    auto code = context->code;
    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, {aggregation});
    auto globalSliceState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "globalSliceState");
    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");

    auto buffers = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffers");
    auto buffersAssignment = VarDeclStatement(buffers).assign(VarRef(globalSliceState).accessRef(call("extractEntries")));
    context->code->variableInitStmts.push_back(buffersAssignment.copy());
    // for (auto& partition : (*partitions.get())) {
    auto getSizeCall = call("size");

    auto partitionIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                    "partitionIndex",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());

    auto partitionLoop = std::make_shared<FOR>(partitionIndex,
                                               (VarRef(partitionIndex) < (VarRef(buffers).accessPtr(getSizeCall))).copy(),
                                               (++VarRef(partitionIndex)).copy());
    {
        auto loopBody = partitionLoop->getCompoundStatement();
        auto buffer = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "buffer");
        auto entries = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entries");
        // buffer = partitions[partitionIndex];
        auto getCall = call("operator[]");
        getCall->addParameter(VarRef(partitionIndex));
        loopBody->addStatement(VarDeclStatement(buffer).assign(VarRef(buffers).accessPtr(getCall)).copy());
        loopBody->addStatement(VarDeclStatement(entries).assign(VarRef(buffer).accessRef(call("getBuffer"))).copy());
        // scan over buffer
        auto recordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
            VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()),
                                        "recordIndex",
                                        DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
                .copy());

        auto scanLoop = std::make_shared<FOR>(
            recordIndex,
            (VarRef(recordIndex) < (VarRef(buffer).accessRef(context->code->tupleBufferGetNumberOfTupleCall))).copy(),
            (++VarRef(recordIndex)).copy());
        {
            auto scanBody = scanLoop->getCompoundStatement();
            auto entry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entry");
            // todo fix entry size
            auto entrySize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(getEntrySize(window))));
            auto entryOffset = Constant(tf->createValueType(DataTypeFactory::createBasicValue(16_u64)));
            auto entryAssignmentStatement =
                VarDeclStatement(entry).assign(TypeCast(VarRef(entries) + ((entrySize * VarRef(recordIndex))) + entryOffset,
                                                        tf->createPointer(partialAggregationEntry.getType())));

            scanBody->addStatement(entryAssignmentStatement.copy());
            // todo currently we assume here that the output schema always has the following fields in the same order.
            // assign start field
            context->getRecordHandler()->registerAttribute(resultSchema->fields[0]->getName(),
                                                           VarRef(globalSlice).accessPtr(call("getStart")).copy());
            // assign end field
            context->getRecordHandler()->registerAttribute(resultSchema->fields[1]->getName(),
                                                           VarRef(globalSlice).accessPtr(call("getEnd")).copy());

            // assign key field
            auto keyDeclarations = getKeyDeclarations(window, tf);
            for (auto key : keyDeclarations) {
                //auto key = partialAggregationEntry.getVariableDeclaration(window->getOnKey()[0]->getFieldName());
                context->getRecordHandler()->registerAttribute(key.getIdentifierName(),
                                                               VarRef(entry).accessPtr(VarRef(key)).copy());
            }

            // assign value field
            for (auto& agg : aggregation) {
                auto value = agg->getPartialAggregate();
                auto finalAggValue = agg->lower(VarRef(entry).accessPtr(VarRef(value)).copy());
                auto asFieldFieldName = agg->getAggregationDescriptor()->as()->as<FieldAccessExpressionNode>()->getFieldName();
                context->getRecordHandler()->registerAttribute(asFieldFieldName, finalAggValue);
            }

            context->code->currentCodeInsertionPoint = scanBody;
        }
        loopBody->addStatement(scanLoop);
    }
    context->code->cleanupStmts.push_back(partitionLoop);

    return false;
}

bool CCodeGenerator::generateCodeForNonKeyedTumblingWindowSink(
    Windowing::LogicalWindowDefinitionPtr window,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> aggregation,
    PipelineContextPtr context,
    SchemaPtr resultSchema) {
    auto tf = getTypeFactory();
    auto code = context->code;
    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, {aggregation});
    auto globalSliceState = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "globalSliceState");
    auto globalSlice = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "globalSlice");
    auto entryPtr = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "ptr");

    auto entry = VariableDeclaration::create(tf->createAnonymusDataType("auto*"), "entry");
    auto blockScope = BlockScopeStatement::create();
    auto entryAssignmentStatement = VarDeclStatement(entry).assign(
        TypeCast(VarRef(globalSliceState).accessPtr(VarRef(entryPtr)), tf->createPointer(partialAggregationEntry.getType())));

    blockScope->addStatement(entryAssignmentStatement.copy());
    // todo currently we assume here that the output schema always has the following fields in the same order.
    // assign start field
    context->getRecordHandler()->registerAttribute(resultSchema->fields[0]->getName(),
                                                   VarRef(globalSlice).accessPtr(call("getStart")).copy());
    // assign end field
    context->getRecordHandler()->registerAttribute(resultSchema->fields[1]->getName(),
                                                   VarRef(globalSlice).accessPtr(call("getEnd")).copy());

    // assign value field
    for (auto& agg : aggregation) {
        auto value = agg->getPartialAggregate();
        auto finalAggValue = agg->lower(VarRef(entry).accessPtr(VarRef(value)).copy());
        auto asFieldFieldName = agg->getAggregationDescriptor()->as()->as<FieldAccessExpressionNode>()->getFieldName();
        context->getRecordHandler()->registerAttribute(asFieldFieldName, finalAggValue);
    }

    code->currentCodeInsertionPoint = blockScope;

    context->code->cleanupStmts.push_back(blockScope);

    return false;
}

/**
 * Code Generation for the window operator
 * @param window windowdefinition
 * @param context pipeline context
 * @param out
 * @return
 */
bool CCodeGenerator::generateCodeForCompleteWindow(
    Windowing::LogicalWindowDefinitionPtr window,
    QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    auto windowOperatorHandlerDeclaration =
        getWindowOperatorHandler(context, context->code->varDeclarationExecutionContext, windowOperatorIndex);
    auto windowHandlerVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");
    auto keyStamp = window->isKeyed() ? window->getKeys()[0]->getStamp() : window->getWindowAggregation()[0]->on()->getStamp();
    auto getWindowHandlerStatement =
        getAggregationWindowHandler(windowOperatorHandlerDeclaration, keyStamp, window->getWindowAggregation()[0]);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());

    auto constStatement = Constant(tf->createValueType(DataTypeFactory::createBasicValue(0_u64)));

    if (context->pipelineName != "SlicingWindowType") {
        context->pipelineName = "CompleteWindowType";
    }

    auto debugDecl = VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), context->pipelineName);
    auto debState = VarDeclStatement(debugDecl).assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(0_u64))));
    context->code->variableInitStmts.push_back(debState.copy());

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");

    NES_ASSERT(!window->getWindowAggregation()[0]->getInputStamp()->isUndefined(), "window input type is undefined");
    NES_ASSERT((!window->getWindowAggregation()[0]->getPartialAggregateStamp()->isUndefined()
                || window->getWindowAggregation()[0]->getType() == Windowing::WindowAggregationDescriptor::Type::Avg
                || window->getWindowAggregation()[0]->getType() == Windowing::WindowAggregationDescriptor::Type::Median),
               "window partial type is undefined");
    NES_ASSERT(!window->getWindowAggregation()[0]->getFinalAggregateStamp()->isUndefined(), "window final type is undefined");

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
    auto keyVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(keyStamp),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "key");
    auto recordHandler = context->getRecordHandler();
    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration = recordHandler->getAttribute(window->getKeys()[0]->getFieldName());
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(keyVariableAttributeDeclaration);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto defaultKeyAssignment = VarDeclStatement(keyVariableDeclaration)
                                        .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(0_s64))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(defaultKeyAssignment));
    }

    // get key handle for current key
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");

    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    // set the default value for window state based on the aggregation function:
    // max: initialize with the lower bound of the data type
    // min: initialize with the upper bound of the data type
    // count & sum: initialize with 0
    // avg : initialize with an empty AVGPartialType
    // median: initialize with a vector of the data type
    switch (window->getWindowAggregation()[0]->getType()) {
        case Windowing::WindowAggregationDescriptor::Type::Min: {
            if (auto intType = DataType::as<Integer>(window->getWindowAggregation()[0]->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(intType, std::to_string(intType->upperBound)))));
            } else if (auto floatType = DataType::as<Float>(window->getWindowAggregation()[0]->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(floatType, std::to_string(floatType->upperBound)))));
            }
            break;
        }
        case Windowing::WindowAggregationDescriptor::Type::Max: {
            if (auto intType = DataType::as<Integer>(window->getWindowAggregation()[0]->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(intType, std::to_string(intType->lowerBound)))));
            } else if (auto floatType = DataType::as<Float>(window->getWindowAggregation()[0]->getPartialAggregateStamp())) {
                getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                    tf->createValueType(DataTypeFactory::createBasicValue(floatType, std::to_string(floatType->lowerBound)))));
            }
            break;
        }
        case Windowing::WindowAggregationDescriptor::Type::Sum: {
            getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
            break;
        }
        case Windowing::WindowAggregationDescriptor::Type::Count: {
            getValueFromKeyHandle.addParameter(ConstantExpressionStatement(
                tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
            break;
        }
        case Windowing::WindowAggregationDescriptor::Type::Avg: {
            // generates the following initial value (T is the aggregationInputType):
            // Windowing::AVGPartialType<T> initialValue;
            auto aggregationInputType = tf->createDataType(window->getWindowAggregation()[0]->getInputStamp());
            auto avgInitialValueDeclaration = VariableDeclaration::create(
                tf->createAnonymusDataType("Windowing::AVGPartialType<" + aggregationInputType->getCode()->code_ + ">"),
                "initialValue");
            context->code->currentCodeInsertionPoint->addStatement(VarDeclStatement(avgInitialValueDeclaration).createCopy());
            getValueFromKeyHandle.addParameter(VarRefStatement(avgInitialValueDeclaration));
            break;
        }
        case Windowing::WindowAggregationDescriptor::Type::Median: {
            // generates the following initial value (T is the aggregationInputType):
            // std::vector<T> initialValue;
            auto aggregationInputType = tf->createDataType(window->getWindowAggregation()[0]->getInputStamp());
            auto medianInitialValueDeclaration = VariableDeclaration::create(
                tf->createAnonymusDataType("std::vector<" + aggregationInputType->getCode()->code_ + ">"),
                "initialValue");
            context->code->currentCodeInsertionPoint->addStatement(VarDeclStatement(medianInitialValueDeclaration).createCopy());
            getValueFromKeyHandle.addParameter(VarRefStatement(medianInitialValueDeclaration));
            break;
        }
        default: {
            NES_FATAL_ERROR("CCodeGenerator: Window Handler - could not cast aggregation type");
        }
    }

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));

    context->code->currentCodeInsertionPoint->addStatement(windowStateVariableStatement.copy());

    // get current timestamp
    // TODO add support for event time
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(window->getWindowType());
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        NES_ASSERT(!context->code->structDeclarationInputTuples.empty(), "invalid number of input tuples");
        auto timeCharacteristicField = timeBasedWindowType->getTimeCharacteristic()->getField()->getName();
        auto tsVariableDeclaration = recordHandler->getAttribute(timeCharacteristicField);

        /**
         * calculateUnitMultiplier => cal to ms
         */
        auto multiplier = timeBasedWindowType->getTimeCharacteristic()->getTimeUnit().getMultiplier();
        //In this case we need to multiply the ts with the multiplier to get ms
        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(tsVariableDeclaration)
            * Constant(tf->createValueType(DataTypeFactory::createBasicValue(multiplier)));
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
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

    auto ifStatementSmallerMinWatermark =
        IF(VarRef(currentTimeVariableDeclaration) < VarRef(minWatermarkVariableDeclaration), Continue());
    context->code->currentCodeInsertionPoint->addStatement(ifStatementSmallerMinWatermark.createCopy());

    // lock slice
    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    //only in debug mode add the key for debugging
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    context->code->currentCodeInsertionPoint->addStatement(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream).copy());

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(currentSliceIndexVariableStatement.copy());

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    auto partialAggregatesVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration).assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(assignment.copy());

    // update partial aggregate
    auto partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    NES_ASSERT(!context->code->structDeclarationInputTuples.empty(), "invalid number of input tuples");
    generatableWindowAggregation->compileLiftCombine(context->code->currentCodeInsertionPoint,
                                                     partialRef,
                                                     context->getRecordHandler());

    // get the slice metadata aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getSliceMetadata = FunctionCallStatement("getSliceMetadata");
    auto getSliceMetadataCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceMetadata);
    auto sliceMetadataDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "sliceMetaData");
    auto sliceAssigment = VarDeclStatement(sliceMetadataDeclaration).assign(getSliceMetadataCall);
    context->code->currentCodeInsertionPoint->addStatement(sliceAssigment.copy());

    auto getSliceCall = FunctionCallStatement("incrementRecordsPerSlice");
    auto updateSliceStatement = VarRef(sliceMetadataDeclaration)[current_slice_ref].accessRef(getSliceCall);
    context->code->currentCodeInsertionPoint->addStatement(updateSliceStatement.createCopy());

    // windowHandler->trigger();
    switch (window->getTriggerPolicy()->getPolicyType()) {
        case Windowing::TriggerType::triggerOnRecord: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->currentCodeInsertionPoint->addStatement(call);
            break;
        }
        case Windowing::TriggerType::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = std::make_shared<BinaryOperatorStatement>(VarRef(windowHandlerVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterWindow(context, windowHandlerVariableDeclration);
    return true;
}

bool CCodeGenerator::generateCodeForCEPIterationOperator(uint64_t minIteration,
                                                         uint64_t maxIteration,
                                                         PipelineContextPtr context) {
    NES_DEBUG("CCodeGenerator::generateCodeForCEPIteration: start generating code for CEPITerations");
    auto tf = getTypeFactory();
    auto handler = CEP::CEPOperatorHandler::create();
    auto index = context->registerOperatorHandler(handler);
    auto recordHandler = context->getRecordHandler();

    NES_DEBUG("CCodeGenerator::generateCodeForCEPIteration: call getCEPOperatorHandler using index: {}", index);
    auto CEPOperatorHandlerDeclaration =
        getCEPIterationOperatorHandler(context, context->code->varDeclarationExecutionContext, index);
    // creates the following line of code
    // auto CEPOperatorHandler = pipelineExecutionContext.getOperatorHandler<CEP::CEPOperatorHandler>(0);
    NES_DEBUG("CCodeGenerator::generateCodeForCEPIteration: got CEPOperatorHandler");

    // for each tuple: call addTuple on CEPOperatorCounter to count occurrences of events
    auto updateCounter = VarRef(CEPOperatorHandlerDeclaration).accessPtr(call("incrementCounter"));
    context->code->currentCodeInsertionPoint->addStatement(updateCounter.copy());
    // creates the following line of code: CEPOperatorHandler.addTuple();

    //check counter if iteration conditions (minIteration and maxIteration) are fulfilled
    auto constantMinIteration = Constant(tf->createValueType(DataTypeFactory::createBasicValue(minIteration)));
    auto constantMaxIteration = Constant(tf->createValueType(DataTypeFactory::createBasicValue(maxIteration)));
    auto checkCounter = VarRef(CEPOperatorHandlerDeclaration).accessPtr(call("getCounter"));
    auto ifStatement = IF(checkCounter <= constantMaxIteration && checkCounter >= constantMinIteration);
    // creates the following line of code: if (CEPOperatorHandler.getCounter() <= 4 && CEPOperatorHandler.getCounter() >= 2)
    // if-state is the condition that needs to be fulfilled to consider the tuple

    NES_DEBUG("CCodeGenerator::generateCodeForCEPIteration: created IF CounterStatement");
    // first, add the head and brackets of the if-Counter statement
    context->code->currentCodeInsertionPoint->addStatement(ifStatement.createCopy());
    // second, move insertion point. the rest of the pipeline will be generated within the brackets of the if-statement
    context->code->currentCodeInsertionPoint = ifStatement.getCompoundStatement();

    NES_DEBUG("CCodeGenerator::generateCodeForCEPIteration: Last Step clearCounter if condition is fulfilled");
    //TODO: this is not 100% correct, (1) we ignore the maxIteration condition with his solution, that would require a time condition that states when the counter needs to be reset
    auto resetCounter = VarRef(CEPOperatorHandlerDeclaration).accessPtr(call("clearCounter"));
    context->code->currentCodeInsertionPoint->addStatement(resetCounter.copy());
    // resets the Operator Counter: code: CEPOperatorHandler.clearCounter();

    return true;
}

bool CCodeGenerator::generateCodeForSlicingWindow(
    Windowing::LogicalWindowDefinitionPtr window,
    QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorId) {
    NES_DEBUG("CCodeGenerator::generateCodeForSlicingWindow with {} pipeline {}", window->toString(), context->pipelineName);
    //NOTE: the distinction currently only happens in the trigger
    context->pipelineName = "SlicingWindowType";
    return generateCodeForCompleteWindow(window, generatableWindowAggregation, context, windowOperatorId);
}

uint64_t CCodeGenerator::generateJoinSetup(Join::LogicalJoinDefinitionPtr join, PipelineContextPtr context, uint64_t id) {
    if (context->arity == PipelineContext::PipelineContextArity::BinaryLeft) {
        return 0;
    }

    auto tf = getTypeFactory();
    NES_ASSERT(join, "invalid join definition");
    NES_ASSERT(!join->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!join->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(join->getLeftJoinKey()->getStamp()->isEquals(join->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(join->getLeftSourceType() != nullptr && !join->getLeftSourceType()->fields.empty(), "left join type is undefined");
    NES_ASSERT(join->getRightSourceType() != nullptr && !join->getRightSourceType()->fields.empty(),
               "right join type is undefined");
    NES_ASSERT(context->arity != PipelineContext::PipelineContextArity::Unary,
               "unary operator detected but join codegen invoked");

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);
    auto handlers = context->getOperatorHandlers();
    NES_ASSERT(handlers.size() == 1, "invalid size");
    int64_t joinOperatorHandlerIndex = 0;
    Join::JoinOperatorHandlerPtr joinOperatorHandler = std::dynamic_pointer_cast<Join::JoinOperatorHandler>(handlers[0]);
    NES_ASSERT(joinOperatorHandler != nullptr, "invalid join handler");

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::JoinOperatorHandler>");
    auto constantOperatorHandlerIndex =
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorHandlerIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto joinDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinDefinition");
    auto getWindowDefinitionCall = call("getJoinDefinition");
    auto windowDefinitionStatement =
        VarDeclStatement(joinDefDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());

    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement =
        VarDeclStatement(resultSchemaDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyType = tf->createDataType(join->getLeftJoinKey()->getStamp());
    auto policy = join->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::TriggerType::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
        NES_WARNING("This mode is not supported anymore");
    } else if (policy->getPolicyType() == Windowing::TriggerType::triggerOnWatermarkChange) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnWatermarkChangeTriggerPolicy::create");
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode={} not implemented", magic_enum::enum_name(policy->getPolicyType()));
    }
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto action = join->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Join::JoinActionType::LazyNestedLoopJoin) {
        auto createTriggerActionCall = call("Join::ExecutableNestedLoopJoinTriggerAction<" + keyType->getCode()->code_
                                            + ", InputTupleLeft, InputTupleRight>::create");
        createTriggerActionCall->addParameter(VarRef(joinDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode={} not implemented", magic_enum::enum_name(action->getActionType()));
    }

    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto joinHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto createAggregationWindowHandlerCall =
        call("Join::JoinHandler<" + keyType->getCode()->code_ + ", InputTupleLeft, InputTupleRight>::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(joinDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    createAggregationWindowHandlerCall->addParameter(VarRef(idParam));

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

uint64_t CCodeGenerator::generateCodeForJoinSinkSetup(Join::LogicalJoinDefinitionPtr join,
                                                      PipelineContextPtr context,
                                                      uint64_t id,
                                                      Join::JoinOperatorHandlerPtr joinOperatorHandler) {
    auto tf = getTypeFactory();
    NES_ASSERT(join, "invalid join definition");
    NES_ASSERT(!join->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!join->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(join->getLeftJoinKey()->getStamp()->isEquals(join->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(join->getLeftSourceType() != nullptr && !join->getLeftSourceType()->fields.empty(), "left join type is undefined");
    NES_ASSERT(join->getRightSourceType() != nullptr && !join->getRightSourceType()->fields.empty(),
               "right join type is undefined");

    auto rightTypeStruct = getStructDeclarationFromSchema("InputTupleRight", join->getRightSourceType());
    auto leftTypeStruct = getStructDeclarationFromSchema("InputTupleLeft", join->getLeftSourceType());
    context->code->structDeclarationInputTuples.emplace_back(leftTypeStruct);

    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    int64_t joinOperatorHandlerIndex = context->registerOperatorHandler(joinOperatorHandler);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::JoinOperatorHandler>");
    auto constantOperatorHandlerIndex =
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorHandlerIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto joinDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinDefinition");
    auto getWindowDefinitionCall = call("getJoinDefinition");
    auto windowDefinitionStatement =
        VarDeclStatement(joinDefDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    setupScope->addStatement(windowDefinitionStatement.copy());

    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement =
        VarDeclStatement(resultSchemaDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyType = tf->createDataType(join->getLeftJoinKey()->getStamp());
    auto policy = join->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::TriggerType::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
        NES_WARNING("This mode is not supported anymore");
    } else if (policy->getPolicyType() == Windowing::TriggerType::triggerOnWatermarkChange) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnWatermarkChangeTriggerPolicy::create");
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode={} not implemented.", magic_enum::enum_name(policy->getPolicyType()));
    }
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto action = join->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Join::JoinActionType::LazyNestedLoopJoin) {
        auto createTriggerActionCall = call("Join::ExecutableNestedLoopJoinTriggerAction<" + keyType->getCode()->code_
                                            + ", InputTupleLeft, InputTupleRight>::create");
        createTriggerActionCall->addParameter(VarRef(joinDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode={} not implemented",
                        std::string(magic_enum::enum_name(action->getActionType())));
    }

    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto joinHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto createAggregationWindowHandlerCall =
        call("Join::JoinHandler<" + keyType->getCode()->code_ + ", InputTupleLeft, InputTupleRight>::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(joinDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    createAggregationWindowHandlerCall->addParameter(VarRef(idParam));

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

uint64_t
CCodeGenerator::generateCodeForBatchJoinHandlerSetup(Join::Experimental::LogicalBatchJoinDefinitionPtr batchJoinDef,
                                                     PipelineContextPtr context,
                                                     uint64_t id,
                                                     Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler) {

    /*
     * Create struct definition, e.g.:
        struct __attribute__((packed)) InputTupleBuild {
            int64_t build$id2;
            int64_t build$value;
        };
     */

    const std::string structNameBuildTuple = "InputTupleBuild";
    auto buildTypeStruct = getStructDeclarationFromSchema(structNameBuildTuple, batchJoinDef->getBuildSchema());
    context->code->structDeclarationInputTuples.emplace_back(buildTypeStruct);

    auto tf = getTypeFactory();
    NES_ASSERT(batchJoinDef, "invalid join definition");
    NES_ASSERT(!batchJoinDef->getBuildJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!batchJoinDef->getProbeJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(batchJoinDef->getBuildJoinKey()->getStamp()->isEquals(batchJoinDef->getProbeJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(batchJoinDef->getBuildSchema() != nullptr && !batchJoinDef->getBuildSchema()->fields.empty(),
               "left join type is undefined");
    NES_ASSERT(batchJoinDef->getProbeSchema() != nullptr && !batchJoinDef->getProbeSchema()->fields.empty(),
               "right join type is undefined");

    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    // initial registration of operator handler to make it available in the pipelines' setup() and execute() functions
    int64_t joinOperatorHandlerIndex = context->registerOperatorHandler(batchJoinOperatorHandler);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto batchJoinOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "batchJoinOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Join::Experimental::BatchJoinOperatorHandler>");
    auto constantOperatorHandlerIndex =
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(joinOperatorHandlerIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(batchJoinOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // get batch join Definition
    auto batchJoinDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "batchJoinDefinition");
    auto getBatchJoinDefinitionCall = call("getBatchJoinDefinition");
    auto batchJoinDefinitionStatement =
        VarDeclStatement(batchJoinDefDeclaration)
            .assign(VarRef(batchJoinOperatorHandlerDeclaration).accessPtr(getBatchJoinDefinitionCall));
    setupScope->addStatement(batchJoinDefinitionStatement.copy());

    // getResultSchema
    auto resultSchemaDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "resultSchema");
    auto getResultSchemaCall = call("getResultSchema");
    auto resultSchemaStatement = VarDeclStatement(resultSchemaDeclaration)
                                     .assign(VarRef(batchJoinOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyType = tf->createDataType(batchJoinDef->getBuildJoinKey()->getStamp());
    // todo jm where does id come from
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    // Join::Experimental::BatchJoinHandler<KeyType, InputTypeBuild>(batchJoinDefinition, id)
    auto batchJoinHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "batchJoinHandler");
    auto createBatchJoinHandlerCall =
        call("Join::Experimental::BatchJoinHandler<" + keyType->getCode()->code_ + ", " + structNameBuildTuple + ">::create");
    createBatchJoinHandlerCall->addParameter(VarRef(batchJoinDefDeclaration));
    createBatchJoinHandlerCall->addParameter(VarRef(idParam));

    auto batchJoinHandlerStatement = VarDeclStatement(batchJoinHandler).assign(createBatchJoinHandlerCall);
    setupScope->addStatement(batchJoinHandlerStatement.copy());

    // batchJoinOperatorHandler->setBatchJoinHandler(batchJoinHandler);
    auto setBatchJoinHandlerCall = call("setBatchJoinHandler");
    setBatchJoinHandlerCall->addParameter(VarRef(batchJoinHandler));
    auto setBatchJoinHandlerStatement = VarRef(batchJoinOperatorHandlerDeclaration).accessPtr(setBatchJoinHandlerCall);
    setupScope->addStatement(setBatchJoinHandlerStatement.copy());

    /*  todo jm think this is not needed yet (we dont have setup function)
    // setup batch join handler
    auto getSharedFromThis = call("shared_from_this");
    auto setUpBatchJoinHandlerCall = call("setup");
    setUpBatchJoinHandlerCall->addParameter(VarRef(context->code->varDeclarationExecutionContext).accessRef(getSharedFromThis));

    auto setupBatchJoinHandlerStatement = VarRef(batchJoinHandler).accessPtr(setUpBatchJoinHandlerCall);
    setupScope->addStatement(setupBatchJoinHandlerStatement.copy());
*/

    return joinOperatorHandlerIndex;
}

bool CCodeGenerator::generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef,
                                         PipelineContextPtr context,
                                         uint64_t operatorHandlerIndex) {
    NES_DEBUG("join input={} aritiy={} out={}",
              context->inputSchema->toString(),
              magic_enum::enum_name(context->arity),
              joinDef->getOutputSchema()->toString());

    auto tf = getTypeFactory();

    if (context->arity == PipelineContext::PipelineContextArity::BinaryLeft) {
        auto rightTypeStruct = getStructDeclarationFromSchema("InputTupleRight", joinDef->getRightSourceType());
        context->code->structDeclarationInputTuples.emplace_back(rightTypeStruct);
    } else {
        auto leftTypeStruct = getStructDeclarationFromSchema("InputTupleLeft", joinDef->getLeftSourceType());
        context->code->structDeclarationInputTuples.emplace_back(leftTypeStruct);
    }

    NES_ASSERT(joinDef, "invalid join definition");
    NES_ASSERT(!joinDef->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!joinDef->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(joinDef->getLeftJoinKey()->getStamp()->isEquals(joinDef->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(joinDef->getLeftSourceType() != nullptr && !joinDef->getLeftSourceType()->fields.empty(),
               "left join type is undefined");
    NES_ASSERT(joinDef->getRightSourceType() != nullptr && !joinDef->getRightSourceType()->fields.empty(),
               "right join type is undefined");
    NES_ASSERT(context->arity != PipelineContext::PipelineContextArity::Unary,
               "unary operator detected but join codegen invoked");

    auto code = context->code;

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");
    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");
    auto windowJoinVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");

    auto windowOperatorHandlerDeclaration =
        getJoinOperatorHandler(context, context->code->varDeclarationExecutionContext, operatorHandlerIndex);

    auto getJoinHandlerStatement = getJoinWindowHandler(windowOperatorHandlerDeclaration,
                                                        joinDef->getLeftJoinKey()->getStamp(),
                                                        "InputTupleLeft",
                                                        "InputTupleRight");
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowJoinVariableDeclration).assign(getJoinHandlerStatement).copy());

    //-------------------------

    auto getWindowManagerStatement = getWindowManager(windowJoinVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    if (context->arity == PipelineContext::PipelineContextArity::BinaryLeft) {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side left");
        auto getWindowStateStatement = getLeftJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    } else if (context->arity == PipelineContext::PipelineContextArity::BinaryRight) {
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
    //TODO this is an ugly hack because we cannot create empty VariableDeclaration and we want it outide the if/else
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "_");
    auto recordHandler = context->getRecordHandler();

    if (context->arity == PipelineContext::PipelineContextArity::BinaryLeft) {
        auto joinKeyFieldName = joinDef->getLeftJoinKey()->getFieldName();
        keyVariableDeclaration =
            VariableDeclaration::create(tf->createDataType(joinDef->getLeftJoinKey()->getStamp()), joinKeyFieldName);

        NES_ASSERT2_FMT(recordHandler->hasAttribute(joinKeyFieldName),
                        "join key is not defined on input tuple << " << joinKeyFieldName);

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);

        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto joinKeyFieldName = joinDef->getRightJoinKey()->getFieldName();

        keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(joinDef->getRightJoinKey()->getStamp()),
                                                             joinDef->getRightJoinKey()->getFieldName());

        NES_ASSERT(recordHandler->hasAttribute(joinKeyFieldName), "join key is not defined on iput tuple");

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    }

    // get key handle for current key
    // auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());

    // access window slice state from state variable via key
    // auto windowState = key_value_handle.value();
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(windowStateVariableStatement.copy());

    // get current timestamp
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(joinDef->getWindowType());
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
        //      auto current_ts = NES::Windowing::getTsFromClock();
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        //      auto current_ts = inputTuples[recordIndex].time //the time value of the key
        //TODO: this has to be changed once we close #1543 and thus we would have 2 times the attribute
        //Extract the name of the window field used for time characteristics
        std::string windowTimeStampFieldName = timeBasedWindowType->getTimeCharacteristic()->getField()->getName();
        if (context->arity == PipelineContext::PipelineContextArity::BinaryRight) {
            NES_DEBUG("windowTimeStampFieldName bin right= {}", windowTimeStampFieldName);

            //Extract the schema of the right side
            auto rightSchema = joinDef->getRightSourceType();
            //Extract the field name without attribute name resolution
            auto trimmedWindowFieldName =
                windowTimeStampFieldName.substr(windowTimeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR),
                                                windowTimeStampFieldName.length());
            //Extract the first field from right schema and trim it to find the schema qualifier for the right side
            //TODO: I know I know this is really not nice but we will fix in with the other issue above
            bool found = false;
            for (auto& field : rightSchema->fields) {
                if (field->getName().find(trimmedWindowFieldName) != std::string::npos) {
                    windowTimeStampFieldName = field->getName();
                    found = true;
                }
            }
            NES_ASSERT(found, " right schema does not contain a timestamp attribute");
        } else {
            NES_DEBUG("windowTimeStampFieldName bin left= {}", windowTimeStampFieldName);
        }

        auto tsVariableReference = recordHandler->getAttribute(windowTimeStampFieldName);

        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(tsVariableReference);
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
    }

    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

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

    // append to the join state
    auto joinStateCall = FunctionCallStatement("append");
    joinStateCall.addParameter(VarRef(currentSliceIndexVariableDeclaration));
    joinStateCall.addParameter(
        VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)]);
    auto getJoinStateCall = VarRef(windowStateVariableDeclaration).accessPtr(joinStateCall);
    context->code->currentCodeInsertionPoint->addStatement(getJoinStateCall.copy());

    // joinHandler->trigger();
    switch (joinDef->getTriggerPolicy()->getPolicyType()) {
        case Windowing::TriggerType::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowJoinVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("CCodeGenerator: Generate code for : {}", context->pipelineName);
    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterJoin(context,
                                        windowJoinVariableDeclration,
                                        context->arity == PipelineContext::PipelineContextArity::BinaryLeft);
    return true;
}

bool CCodeGenerator::generateCodeForJoinBuild(Join::LogicalJoinDefinitionPtr joinDef,
                                              PipelineContextPtr context,
                                              Join::JoinOperatorHandlerPtr joinOperatorHandler,
                                              QueryCompilation::JoinBuildSideType buildSide) {
    NES_DEBUG("join input={} aritiy={} out={}",
              context->inputSchema->toString(),
              magic_enum::enum_name(buildSide),
              joinDef->getOutputSchema()->toString());

    auto tf = getTypeFactory();

    if (buildSide == QueryCompilation::JoinBuildSideType::Left) {
        auto rightTypeStruct = getStructDeclarationFromSchema("InputTupleRight", joinDef->getRightSourceType());
        context->code->structDeclarationInputTuples.emplace_back(rightTypeStruct);
    } else {
        auto leftTypeStruct = getStructDeclarationFromSchema("InputTupleLeft", joinDef->getLeftSourceType());
        context->code->structDeclarationInputTuples.emplace_back(leftTypeStruct);
    }

    NES_ASSERT(joinDef, "invalid join definition");
    NES_ASSERT(!joinDef->getLeftJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!joinDef->getRightJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(joinDef->getLeftJoinKey()->getStamp()->isEquals(joinDef->getRightJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(joinDef->getLeftSourceType() != nullptr && !joinDef->getLeftSourceType()->fields.empty(),
               "left join type is undefined");
    NES_ASSERT(joinDef->getRightSourceType() != nullptr && !joinDef->getRightSourceType()->fields.empty(),
               "right join type is undefined");

    auto code = context->code;

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");
    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");
    auto windowJoinVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "joinHandler");
    auto operatorHandlerIndex = context->registerOperatorHandler(joinOperatorHandler);
    auto windowOperatorHandlerDeclaration =
        getJoinOperatorHandler(context, context->code->varDeclarationExecutionContext, operatorHandlerIndex);

    if (buildSide == QueryCompilation::JoinBuildSideType::Left) {
        auto getJoinHandlerStatement = getJoinWindowHandler(windowOperatorHandlerDeclaration,
                                                            joinDef->getLeftJoinKey()->getStamp(),
                                                            "InputTuple",
                                                            "InputTupleRight");
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowJoinVariableDeclration).assign(getJoinHandlerStatement).copy());
    } else {
        auto getJoinHandlerStatement = getJoinWindowHandler(windowOperatorHandlerDeclaration,
                                                            joinDef->getLeftJoinKey()->getStamp(),
                                                            "InputTupleLeft",
                                                            "InputTuple");
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowJoinVariableDeclration).assign(getJoinHandlerStatement).copy());
    }

    //-------------------------

    auto getWindowManagerStatement = getWindowManager(windowJoinVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    if (buildSide == QueryCompilation::JoinBuildSideType::Left) {
        NES_DEBUG("CCodeGenerator::generateCodeForJoin generate code for side left");
        auto getWindowStateStatement = getLeftJoinState(windowJoinVariableDeclration);
        context->code->variableInitStmts.emplace_back(
            VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    } else if (buildSide == QueryCompilation::JoinBuildSideType::Right) {
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
    //TODO this is an ugly hack because we cannot create empty VariableDeclaration and we want it outide the if/else
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "_");
    auto recordHandler = context->getRecordHandler();

    if (buildSide == QueryCompilation::JoinBuildSideType::Left) {
        auto joinKeyFieldName = joinDef->getLeftJoinKey()->getFieldName();
        keyVariableDeclaration =
            VariableDeclaration::create(tf->createDataType(joinDef->getLeftJoinKey()->getStamp()), joinKeyFieldName + "leftKey");

        NES_ASSERT2_FMT(recordHandler->hasAttribute(joinKeyFieldName),
                        "join key is not defined on input tuple << " << joinKeyFieldName);

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);

        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto joinKeyFieldName = joinDef->getRightJoinKey()->getFieldName();

        keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(joinDef->getRightJoinKey()->getStamp()),
                                                             joinDef->getRightJoinKey()->getFieldName() + "rightKey");

        NES_ASSERT(recordHandler->hasAttribute(joinKeyFieldName), "join key is not defined on input tuple");

        auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    }

    // get key handle for current key
    // auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());

    // access window slice state from state variable via key
    // auto windowState = key_value_handle.value();
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(windowStateVariableStatement.copy());

    // get current timestamp
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(joinDef->getWindowType());
    if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime) {
        //      auto current_ts = NES::Windowing::getTsFromClock();
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        //      auto current_ts = inputTuples[recordIndex].time //the time value of the key
        //TODO: this has to be changed once we close #1543 and thus we would have 2 times the attribute
        //Extract the name of the window field used for time characteristics
        std::string windowTimeStampFieldName = timeBasedWindowType->getTimeCharacteristic()->getField()->getName();
        if (buildSide == QueryCompilation::JoinBuildSideType::Right) {
            NES_DEBUG("windowTimeStampFieldName bin right={}", windowTimeStampFieldName);

            //Extract the schema of the right side
            auto rightSchema = joinDef->getRightSourceType();
            //Extract the field name without attribute name resolution
            auto trimmedWindowFieldName =
                windowTimeStampFieldName.substr(windowTimeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR),
                                                windowTimeStampFieldName.length());
            //Extract the first field from right schema and trim it to find the schema qualifier for the right side
            //TODO: I know I know this is really not nice but we will fix in with the other issue above
            bool found = false;
            for (auto& field : rightSchema->fields) {
                if (field->getName().find(trimmedWindowFieldName) != std::string::npos) {
                    windowTimeStampFieldName = field->getName();
                    found = true;
                }
            }
            NES_ASSERT(found, " right schema does not contain a timestamp attribute");
        } else {
            NES_DEBUG("windowTimeStampFieldName bin left= {}", windowTimeStampFieldName);
        }

        auto tsVariableReference = recordHandler->getAttribute(windowTimeStampFieldName);

        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(tsVariableReference);
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
    }

    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

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

    // append to the join state
    auto joinStateCall = FunctionCallStatement("append");
    joinStateCall.addParameter(VarRef(currentSliceIndexVariableDeclaration));
    joinStateCall.addParameter(
        VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)]);
    auto getJoinStateCall = VarRef(windowStateVariableDeclaration).accessPtr(joinStateCall);
    context->code->currentCodeInsertionPoint->addStatement(getJoinStateCall.copy());

    // joinHandler->trigger();
    switch (joinDef->getTriggerPolicy()->getPolicyType()) {
        case Windowing::TriggerType::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            call = std::make_shared<BinaryOperatorStatement>(VarRef(windowJoinVariableDeclration).accessPtr(trigger));
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("CCodeGenerator: Generate code for {}:", context->pipelineName);
    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterJoin(context,
                                        windowJoinVariableDeclration,
                                        buildSide == QueryCompilation::JoinBuildSideType::Left);
    return true;
}

bool CCodeGenerator::generateCodeForBatchJoinBuild(Join::Experimental::LogicalBatchJoinDefinitionPtr batchJoinDef,
                                                   PipelineContextPtr context,
                                                   Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler) {
    NES_DEBUG("batch join input={} out={}", context->inputSchema->toString(), batchJoinDef->getOutputSchema()->toString());

    auto tf = getTypeFactory();
    const std::string structNameBuildTuple = "InputTuple";// we reuse the struct defined by generateCodeForScan

    NES_ASSERT(batchJoinDef, "invalid join definition");
    NES_ASSERT(!batchJoinDef->getBuildJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!batchJoinDef->getProbeJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(batchJoinDef->getBuildJoinKey()->getStamp()->isEquals(batchJoinDef->getProbeJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(batchJoinDef->getBuildSchema() != nullptr && !batchJoinDef->getBuildSchema()->fields.empty(),
               "left join type is undefined");
    NES_ASSERT(batchJoinDef->getProbeSchema() != nullptr && !batchJoinDef->getProbeSchema()->fields.empty(),
               "right join type is undefined");

    auto code = context->code;

    // "auto batchJoinHandler = pipelineExecutionContext.getBatchJoinHandler<int64_t, InputTupleBuild>()";
    auto batchJoinHandlerVariableDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "batchJoinHandler");

    int64_t operatorHandlerIndex = context->getHandlerIndex(batchJoinOperatorHandler);
    auto batchJoinOperatorHandlerDeclaration =
        getBatchJoinOperatorHandler(context, context->code->varDeclarationExecutionContext, operatorHandlerIndex);

    auto getJoinHandlerStatement = getBatchJoinHandler(batchJoinOperatorHandlerDeclaration,
                                                       batchJoinDef->getBuildJoinKey()->getStamp(),
                                                       structNameBuildTuple);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(batchJoinHandlerVariableDeclaration).assign(getJoinHandlerStatement).copy());

    // auto hashTable = batchJoinHandler.getHashTable();
    auto hashTableVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "hashTable");
    auto getHashTableCall = call("getHashTable");
    auto getHashTableStatement = VarRef(batchJoinHandlerVariableDeclaration).accessPtr(getHashTableCall);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(hashTableVarDeclaration).assign(getHashTableStatement).copy());
    //-------------------------

    /**
    * within the loop
    */
    // Read key value from record
    // "int64_t key = inputTuplesBuild[recordIndex].key;"
    auto recordHandler = context->getRecordHandler();
    auto joinKeyFieldName = batchJoinDef->getBuildJoinKey()->getFieldName();
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(batchJoinDef->getBuildJoinKey()->getStamp()),
                                                              joinKeyFieldName + "_buildKey");

    NES_ASSERT2_FMT(recordHandler->hasAttribute(joinKeyFieldName),
                    "join key is not defined on input tuple << " << joinKeyFieldName);

    auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);

    auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(joinKeyReference);
    context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());

    // insert tuple into hashTable
    // "hashTable->insert(key, inputTuples[recordIndex]);"
    auto insertCall = call("insert");
    auto currentTupleStatement = VarRef(code->varDeclarationInputTuples)[VarRef(code->varDeclarationRecordIndex)];
    insertCall->addParameter(VarRef(keyVariableDeclaration));
    insertCall->addParameter(currentTupleStatement);
    context->code->currentCodeInsertionPoint->addStatement(VarRef(hashTableVarDeclaration).accessPtr(insertCall).copy());

    NES_DEBUG("CCodeGenerator: Generate code for {}", context->pipelineName);

    return true;
}

bool CCodeGenerator::generateCodeForBatchJoinProbe(Join::Experimental::LogicalBatchJoinDefinitionPtr batchJoinDef,
                                                   PipelineContextPtr context,
                                                   Join::Experimental::BatchJoinOperatorHandlerPtr batchJoinOperatorHandler) {
    NES_DEBUG("batch join input={} out={}", context->inputSchema->toString(), batchJoinDef->getOutputSchema()->toString());

    NES_ASSERT(batchJoinDef, "invalid join definition");
    NES_ASSERT(!batchJoinDef->getBuildJoinKey()->getStamp()->isUndefined(), "left join key is undefined");
    NES_ASSERT(!batchJoinDef->getProbeJoinKey()->getStamp()->isUndefined(), "right join key is undefined");
    NES_ASSERT(batchJoinDef->getBuildJoinKey()->getStamp()->isEquals(batchJoinDef->getProbeJoinKey()->getStamp()),
               "left join key is not the same type as right join key");
    NES_ASSERT(batchJoinDef->getBuildSchema() != nullptr && !batchJoinDef->getBuildSchema()->fields.empty(),
               "left join type is undefined");
    NES_ASSERT(batchJoinDef->getProbeSchema() != nullptr && !batchJoinDef->getProbeSchema()->fields.empty(),
               "right join type is undefined");

    auto tf = getTypeFactory();
    auto code = context->code;

    // define the build struct (instants of this struct are stored in the hash table)
    const std::string structNameBuildTuple = "InputTupleBuild";
    auto buildTypeStruct = getStructDeclarationFromSchema(structNameBuildTuple, batchJoinDef->getBuildSchema());
    context->code->structDeclarationInputTuples.emplace_back(buildTypeStruct);

    // "auto batchJoinHandler = pipelineExecutionContext.getBatchJoinHandler<int64_t, InputTupleBuild>();"
    auto batchJoinHandlerVariableDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "batchJoinHandler");
    auto operatorHandlerIndex = context->registerOperatorHandler(batchJoinOperatorHandler);
    auto batchJoinOperatorHandlerDeclaration =
        getBatchJoinOperatorHandler(context, context->code->varDeclarationExecutionContext, operatorHandlerIndex);

    auto getJoinHandlerStatement = getBatchJoinHandler(batchJoinOperatorHandlerDeclaration,
                                                       batchJoinDef->getBuildJoinKey()->getStamp(),
                                                       structNameBuildTuple);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(batchJoinHandlerVariableDeclaration).assign(getJoinHandlerStatement).copy());

    // auto hashTable = batchJoinHandler.getHashTable();
    auto hashTableVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "hashTable");
    auto getHashTableCall = call("getHashTable");
    auto getHashTableStatement = VarRef(batchJoinHandlerVariableDeclaration).accessPtr(getHashTableCall);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(hashTableVarDeclaration).assign(getHashTableStatement).copy());
    //-------------------------

    /**
    * within the loop
    */
    // Check validity of join key in record handler
    // (should be "inputTuplesProbe[recordIndex].key")
    auto recordHandler = context->getRecordHandler();
    auto joinKeyFieldName = batchJoinDef->getProbeJoinKey()->getFieldName();
    NES_ASSERT2_FMT(recordHandler->hasAttribute(joinKeyFieldName),
                    "join key is not defined on input tuple << " << joinKeyFieldName);
    auto joinKeyReference = recordHandler->getAttribute(joinKeyFieldName);

    // if a join partner for current tuple can be found proceed to emit
    // "if (hashTable->contains(inputTuples[recordIndex].key)) { ... }" <- contain function return a boolean true on success
    auto containsCall = FunctionCallStatement("contains");
    containsCall.addParameter(joinKeyReference);
    auto containsStatement = VarRef(hashTableVarDeclaration).accessPtr(containsCall);
    auto ifJoinPartnerFoundStatement = IF(containsStatement);
    code->currentCodeInsertionPoint->addStatement(ifJoinPartnerFoundStatement.createCopy());
    code->currentCodeInsertionPoint = ifJoinPartnerFoundStatement.getCompoundStatement();

    // extract joinPartner
    // "InputTupleBuild joinPartner = hashTable->find(inputTuples[recordIndex].key);"
    auto joinPartnerVariableDeclaration = VariableDeclaration::create(tf->createUserDefinedType(buildTypeStruct), "joinPartner");

    auto findJoinPartnerCall = FunctionCallStatement("find");
    findJoinPartnerCall.addParameter(joinKeyReference);
    auto findJoinPartnerStatement = VarRef(hashTableVarDeclaration).accessPtr(findJoinPartnerCall);

    auto joinPartnerDeclaration = VarDeclStatement(joinPartnerVariableDeclaration).assign(findJoinPartnerStatement);
    code->currentCodeInsertionPoint->addStatement(joinPartnerDeclaration.copy());

    // register all fields of "InputTupleBuild joinPartner" in operator handler for use in emit
    for (const AttributeFieldPtr& field : batchJoinDef->getBuildSchema()->fields) {
        auto fieldVarDeclaration = VariableDeclaration::create(field->getDataType(), field->getName());
        auto joinPartnerAccessStatement = VarRef(joinPartnerVariableDeclaration).accessRef(VarRef(fieldVarDeclaration));
        recordHandler->registerAttribute(field->getName(), joinPartnerAccessStatement.copy());
    }

    NES_DEBUG("CCodeGenerator: Generate code for {}:", context->pipelineName);

    return true;
}

bool CCodeGenerator::generateCodeForCombiningWindow(
    Windowing::LogicalWindowDefinitionPtr window,
    QueryCompilation::GeneratableOperators::GeneratableWindowAggregationPtr generatableWindowAggregation,
    PipelineContextPtr context,
    uint64_t windowOperatorIndex) {
    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for combine window.");
    auto code = context->code;

    if (window->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Type::Combining) {
        context->pipelineName = "combiningWindowType";
    } else {
        context->pipelineName = "sliceMergingWindowType";
    }

    auto debugDecl = VariableDeclaration::create(tf->createAnonymusDataType("uint64_t"), context->pipelineName);
    auto debState = VarDeclStatement(debugDecl).assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(0_u64))));
    context->code->variableInitStmts.push_back(debState.copy());

    auto windowManagerVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowStateVar");
    auto windowOperatorHandlerDeclaration =
        getWindowOperatorHandler(context, context->code->varDeclarationExecutionContext, windowOperatorIndex);
    auto windowHandlerVariableDeclration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");

    auto keyStamp = window->isKeyed() ? window->getKeys()[0]->getStamp() : window->getWindowAggregation()[0]->on()->getStamp();

    auto getWindowHandlerStatement =
        getAggregationWindowHandler(windowOperatorHandlerDeclaration, keyStamp, window->getWindowAggregation()[0]);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());

    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(
        VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());

    /**
   * within the loop
   */

    //get min watermark
    auto minWatermarkVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "minWatermark");
    auto getMinWatermarkStateVariable = FunctionCallStatement("getMinWatermark");
    auto minWatermarkHandlerVariableStatement =
        VarDeclStatement(minWatermarkVariableDeclaration)
            .assign(VarRef(windowHandlerVariableDeclration).accessPtr(getMinWatermarkStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(minWatermarkHandlerVariableStatement.copy());

    //        NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>* state_variable = (NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>*) state_var;
    auto stateVariableDeclaration =
        VariableDeclaration::create(tf->createPointer(tf->createAnonymusDataType(
                                        "NES::Runtime::StateVariable<int64_t, NES::Windowing::WindowSliceStore<int64_t>*>")),
                                    "state_variable");

    auto stateVarDeclarationStatement =
        VarDeclStatement(stateVariableDeclaration)
            .assign(TypeCast(VarRef(windowStateVarDeclaration), stateVariableDeclaration.getDataType()));
    context->code->currentCodeInsertionPoint->addStatement(stateVarDeclarationStatement.copy());

    // Read key value from record
    //        int64_t key = windowTuples[recordIndex].key;

    //TODO this is not nice but we cannot create an empty one or a ptr
    auto keyVariableDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "key");
    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration = context->getRecordHandler()->getAttribute(window->getKeys()[0]->getFieldName());
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration).assign(keyVariableAttributeDeclaration);
        context->code->currentCodeInsertionPoint->addStatement(keyVariableAttributeStatement.copy());
    } else {
        auto defaultKeyAssignment = VarDeclStatement(keyVariableDeclaration)
                                        .assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(uint64_t(0)))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(defaultKeyAssignment));
    }

    // get key handle for current key
    //        auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement =
        VarDeclStatement(keyHandlerVariableDeclaration).assign(VarRef(stateVariableDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(keyHandlerVariableStatement.copy());

    //    auto windowState = key_value_handle.valueOrDefault(0);
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(
        ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(uint64_t(0)))));
    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(windowStateVariableStatement));

    // get current timestamp
    auto currentTimeVariableDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start");
    auto recordStartAttributeRef = context->getRecordHandler()->getAttribute(
        context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start");

    if (Windowing::WindowType::asTimeBasedWindowType(window->getWindowType())->getTimeCharacteristic()->getType()
        == Windowing::TimeCharacteristic::Type::IngestionTime) {
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(recordStartAttributeRef);
        context->code->currentCodeInsertionPoint->addStatement(getCurrentTsStatement.copy());
    } else {
        currentTimeVariableDeclaration = VariableDeclaration::create(
            tf->createAnonymusDataType("auto"),
            context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start");
        auto tsVariableDeclarationStatement =
            VarDeclStatement(currentTimeVariableDeclaration).assign(recordStartAttributeRef->copy());
        context->code->currentCodeInsertionPoint->addStatement(tsVariableDeclarationStatement.copy());
    }

    //        if (ts < minWatermark)
    //          {continue;}
    auto ifStatementSmallerMinWatermark =
        IF(VarRef(currentTimeVariableDeclaration) < VarRef(minWatermarkVariableDeclaration), Continue());
    context->code->currentCodeInsertionPoint->addStatement(ifStatementSmallerMinWatermark.createCopy());

    // get current timestamp
    auto currentCntVariable =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"),
                                    context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "cnt");
    auto recordCntFieldRef = context->getRecordHandler()->getAttribute(
        context->getInputSchema()->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "cnt");
    auto getCurrentCntStatement = VarDeclStatement(currentCntVariable).assign(recordCntFieldRef);
    context->code->currentCodeInsertionPoint->addStatement(getCurrentCntStatement.copy());

    // lock slice
    // auto lock = std::unique_lock(stateVariable->mutex());
    auto uniqueLockVariable = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "lock");
    auto uniqueLockCtor = FunctionCallStatement("std::unique_lock");
    auto stateMutex = FunctionCallStatement("mutex");
    uniqueLockCtor.addParameter(
        std::make_shared<BinaryOperatorStatement>(VarRef(windowStateVariableDeclaration).accessPtr(stateMutex)));
    context->code->currentCodeInsertionPoint->addStatement(
        std::make_shared<BinaryOperatorStatement>(VarDeclStatement(uniqueLockVariable).assign(uniqueLockCtor)));

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    sliceStream.addParameter(VarRef(keyVariableDeclaration));
    auto sliceStreamStatement = VarRef(windowManagerVarDeclaration).accessPtr(sliceStream).copy();
    context->code->currentCodeInsertionPoint->addStatement(sliceStreamStatement);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration =
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement =
        VarDeclStatement(currentSliceIndexVariableDeclaration).assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(currentSliceIndexVariableStatement.copy());

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration).assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    generatableWindowAggregation->compileLiftCombine(context->code->currentCodeInsertionPoint,
                                                     partialRef,
                                                     context->getRecordHandler());

    // get the slice metadata aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getSliceMetadata = FunctionCallStatement("getSliceMetadata");
    auto getSliceMetadataCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceMetadata);
    VariableDeclaration sliceMetadataDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto&"), "sliceMetaData");
    auto sliceAssigment = VarDeclStatement(sliceMetadataDeclaration).assign(getSliceMetadataCall);
    context->code->currentCodeInsertionPoint->addStatement(sliceAssigment.copy());

    auto getSliceCall = FunctionCallStatement("incrementRecordsPerSliceByValue");
    getSliceCall.addParameter(VarRef(currentCntVariable));
    auto updateSliceStatement = VarRef(sliceMetadataDeclaration)[current_slice_ref].accessRef(getSliceCall);
    context->code->currentCodeInsertionPoint->addStatement(updateSliceStatement.createCopy());

    // windowHandler->trigger();
    switch (window->getTriggerPolicy()->getPolicyType()) {
        case Windowing::TriggerType::triggerOnRecord: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = VarRef(windowHandlerVariableDeclration).accessPtr(trigger).copy();
            context->code->currentCodeInsertionPoint->addStatement(call);
            break;
        }
        case Windowing::TriggerType::triggerOnBuffer: {
            auto trigger = FunctionCallStatement("trigger");
            auto call = VarRef(windowHandlerVariableDeclration).accessPtr(trigger).copy();
            context->code->cleanupStmts.push_back(call);
            break;
        }
        default: {
            break;
        }
    }

    NES_DEBUG("CCodeGenerator: Generate code for {}", context->pipelineName);

    // Generate code for watermark updater
    // i.e., calling updateAllMaxTs
    generateCodeForWatermarkUpdaterWindow(context, windowHandlerVariableDeclration);
    return true;
}

void CCodeGenerator::generateCodeForAggregationInitialization(const BlockScopeStatementPtr& setupScope,
                                                              const VariableDeclaration& executableAggregation,
                                                              const VariableDeclaration& partialAggregateInitialValue,
                                                              const GeneratableDataTypePtr& aggregationInputType,
                                                              const Windowing::WindowAggregationDescriptorPtr& aggregation) {
    FunctionCallStatementPtr createAggregateCall;
    auto tf = getTypeFactory();

    // If the aggregate is Avg, we initialize the partialAggregate with an empty AVGPartialType<InputType>
    if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Type::Avg) {
        auto partialAggregateInitStatement =
            VarDeclStatement(partialAggregateInitialValue)
                .assign(call("Windowing::AVGPartialType<" + aggregationInputType->getCode()->code_ + ">"));
        setupScope->addStatement(partialAggregateInitStatement.copy());
        createAggregateCall = call("Windowing::ExecutableAVGAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
    } else if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Type::Median) {
        auto partialAggregateInitStatement = VarDeclStatement(partialAggregateInitialValue)
                                                 .assign(call("std::vector<" + aggregationInputType->getCode()->code_ + ">"));
        setupScope->addStatement(partialAggregateInitStatement.copy());
        createAggregateCall =
            call("Windowing::ExecutableMedianAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
    } else {
        // If the the aggregate is not Avg or median, we initialize the partialAggregate with 0
        auto partialAggregateInitStatement =
            VarDeclStatement(partialAggregateInitialValue)
                .assign(Constant(
                    tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0)))));

        if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Type::Sum) {
            createAggregateCall =
                call("Windowing::ExecutableSumAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Type::Count) {
            createAggregateCall =
                call("Windowing::ExecutableCountAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Type::Min) {
            // If the the aggregate is Min, we initialize the partialAggregate with the upper bound of the type of the aggregated field
            std::string upperBoundstr;
            if (auto intType = DataType::as<Integer>(aggregation->getPartialAggregateStamp())) {
                upperBoundstr = std::to_string(intType->upperBound);
            } else if (auto floatType = DataType::as<Float>(aggregation->getPartialAggregateStamp())) {
                upperBoundstr = std::to_string(floatType->upperBound);
            }
            partialAggregateInitStatement =
                VarDeclStatement(partialAggregateInitialValue)
                    .assign(Constant(
                        tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), upperBoundstr))));

            createAggregateCall =
                call("Windowing::ExecutableMinAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else if (aggregation->getType() == Windowing::WindowAggregationDescriptor::Type::Max) {
            // If the the aggregate is Max, we initialize the partialAggregate with the lower bound of the type of the aggregated field
            std::string lowerBoundStr;
            if (auto intType = DataType::as<Integer>(aggregation->getPartialAggregateStamp())) {
                lowerBoundStr = std::to_string(intType->lowerBound);
            } else if (auto floatType = DataType::as<Float>(aggregation->getPartialAggregateStamp())) {
                lowerBoundStr = std::to_string(floatType->lowerBound);
            }
            partialAggregateInitStatement =
                VarDeclStatement(partialAggregateInitialValue)
                    .assign(Constant(
                        tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), lowerBoundStr))));

            createAggregateCall =
                call("Windowing::ExecutableMaxAggregation<" + aggregationInputType->getCode()->code_ + ">::create");
        } else {
            NES_FATAL_ERROR("Aggregation Handler: aggregation={} not implemented",
                            std::string(magic_enum::enum_name(aggregation->getType())));
        }
        // add the partial aggregation initialization to the code
        setupScope->addStatement(partialAggregateInitStatement.copy());
    }

    // add the executable aggregation initialization to the code
    auto statement = VarDeclStatement(executableAggregation).assign(createAggregateCall);
    setupScope->addStatement(statement.copy());
}

uint64_t
CCodeGenerator::generateNonKeyedSliceMergingOperatorSetup(Windowing::LogicalWindowDefinitionPtr window,
                                                          PipelineContextPtr context,
                                                          uint64_t id,
                                                          uint64_t windowOperatorIndex,
                                                          std::vector<GeneratableOperators::GeneratableWindowAggregationPtr>) {

    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::NonKeyedSliceMergingOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
                                         .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    // create struct for entry
    // consists of keys and aggregation values
    //StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, windowAggregation);
    //context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);

    uint64_t aggValueSize = getAggregationValueSize(window);
    auto constantValueSize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(aggValueSize)));

    auto setupWindowHandler = call("setup");
    setupWindowHandler->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    setupWindowHandler->addParameter(constantValueSize);
    setupScope->addStatement(VarRef(windowOperatorHandlerDeclaration).accessPtr(setupWindowHandler).createCopy());
    return 0;
}

uint64_t
CCodeGenerator::generateKeyedSliceMergingOperatorSetup(Windowing::LogicalWindowDefinitionPtr window,
                                                       PipelineContextPtr context,
                                                       uint64_t id,
                                                       uint64_t windowOperatorIndex,
                                                       std::vector<GeneratableOperators::GeneratableWindowAggregationPtr>) {

    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::KeyedSliceMergingOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
                                         .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    // create struct for entry
    // consists of keys and aggregation values
    //StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, windowAggregation);
    //context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);

    auto createCall = call("std::make_shared<Experimental::HashMapFactory>");
    auto getBufferManagerCall = call("getBufferManager");
    createCall->addParameter(VarRefStatement(context->code->varDeclarationExecutionContext).accessRef(getBufferManagerCall));

    uint64_t keysSize = getKeySpaceSize(window);
    uint64_t aggValueSize = getAggregationValueSize(window);
    auto constantKeySize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(keysSize)));
    createCall->addParameter(constantKeySize);
    auto constantValueSize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(aggValueSize)));
    createCall->addParameter(constantValueSize);

    auto constantNumberOfEntries = Constant(tf->createValueType(DataTypeFactory::createBasicValue(100000_u64)));
    createCall->addParameter(constantNumberOfEntries);

    auto hashMapFactory = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "hashMapFactory");

    auto hashMapFactoryStatement = VarDeclStatement(hashMapFactory).assign(createCall);
    setupScope->addStatement(hashMapFactoryStatement.copy());

    auto setupWindowHandler = call("setup");
    setupWindowHandler->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    setupWindowHandler->addParameter(VarRef(hashMapFactory));
    setupScope->addStatement(VarRef(windowOperatorHandlerDeclaration).accessPtr(setupWindowHandler).createCopy());
    return 0;
}

uint64_t
CCodeGenerator::generateKeyedSlidingWindowOperatorSetup(Windowing::LogicalWindowDefinitionPtr window,
                                                        PipelineContextPtr context,
                                                        uint64_t id,
                                                        uint64_t windowOperatorIndex,
                                                        std::vector<GeneratableOperators::GeneratableWindowAggregationPtr>) {

    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
                                         .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    // create struct for entry
    // consists of keys and aggregation values
    //StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, windowAggregation);
    //context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);

    auto createCall = call("std::make_shared<Experimental::HashMapFactory>");
    auto getBufferManagerCall = call("getBufferManager");
    createCall->addParameter(VarRefStatement(context->code->varDeclarationExecutionContext).accessRef(getBufferManagerCall));

    uint64_t keysSize = getKeySpaceSize(window);
    uint64_t aggValueSize = getAggregationValueSize(window);
    auto constantKeySize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(keysSize)));
    createCall->addParameter(constantKeySize);
    auto constantValueSize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(aggValueSize)));
    createCall->addParameter(constantValueSize);

    auto constantNumberOfEntries = Constant(tf->createValueType(DataTypeFactory::createBasicValue(100000_u64)));
    createCall->addParameter(constantNumberOfEntries);

    auto hashMapFactory = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "hashMapFactory");

    auto hashMapFactoryStatement = VarDeclStatement(hashMapFactory).assign(createCall);
    setupScope->addStatement(hashMapFactoryStatement.copy());

    auto setupWindowHandler = call("setup");
    setupWindowHandler->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    setupWindowHandler->addParameter(VarRef(hashMapFactory));
    setupScope->addStatement(VarRef(windowOperatorHandlerDeclaration).accessPtr(setupWindowHandler).createCopy());
    return 0;
}

uint64_t
CCodeGenerator::generateNonKeyedSlidingWindowOperatorSetup(Windowing::LogicalWindowDefinitionPtr window,
                                                           PipelineContextPtr context,
                                                           uint64_t id,
                                                           uint64_t windowOperatorIndex,
                                                           std::vector<GeneratableOperators::GeneratableWindowAggregationPtr>) {

    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));
    auto pipelineExecutionContextType = tf->createAnonymusDataType("Runtime::Execution::PipelineExecutionContext");
    VariableDeclaration varDeclarationPipelineExecutionContext =
        VariableDeclaration::create(tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");

    context->code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
                                         .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    uint64_t aggValueSize = getAggregationValueSize(window);
    auto constantValueSize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(aggValueSize)));
    auto setupWindowHandler = call("setup");
    setupWindowHandler->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    setupWindowHandler->addParameter(constantValueSize);
    setupScope->addStatement(VarRef(windowOperatorHandlerDeclaration).accessPtr(setupWindowHandler).createCopy());
    return 0;
}

uint64_t CCodeGenerator::generateKeyedThreadLocalPreAggregationSetup(
    Windowing::LogicalWindowDefinitionPtr window,
    PipelineContextPtr context,
    uint64_t id,
    uint64_t windowOperatorIndex,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {

    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall =
        call("getOperatorHandler<Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
                                         .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    // create struct for entry
    // consists of keys and aggregation values
    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, windowAggregation);
    context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);

    auto createCall = call("std::make_shared<Experimental::HashMapFactory>");
    auto getBufferManagerCall = call("getBufferManager");
    createCall->addParameter(VarRefStatement(context->code->varDeclarationExecutionContext).accessRef(getBufferManagerCall));

    uint64_t keysSize = getKeySpaceSize(window);
    uint64_t aggValueSize = getAggregationValueSize(window);
    auto constantKeySize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(keysSize)));
    createCall->addParameter(constantKeySize);
    auto constantValueSize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(aggValueSize)));
    createCall->addParameter(constantValueSize);

    auto constantNumberOfEntries = Constant(tf->createValueType(DataTypeFactory::createBasicValue(100000_u64)));
    createCall->addParameter(constantNumberOfEntries);

    auto hashMapFactory = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "hashMapFactory");

    auto hashMapFactoryStatement = VarDeclStatement(hashMapFactory).assign(createCall);
    setupScope->addStatement(hashMapFactoryStatement.copy());

    auto setupWindowHandler = call("setup");
    setupWindowHandler->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    setupWindowHandler->addParameter(VarRef(hashMapFactory));
    setupScope->addStatement(VarRef(windowOperatorHandlerDeclaration).accessPtr(setupWindowHandler).createCopy());
    return 0;
}

uint64_t CCodeGenerator::generateNonKeyedThreadLocalPreAggregationSetup(
    Windowing::LogicalWindowDefinitionPtr window,
    SchemaPtr,
    PipelineContextPtr context,
    uint64_t id,
    uint64_t windowOperatorIndex,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {

    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall =
        call("getOperatorHandler<Windowing::Experimental::NonKeyedThreadLocalPreAggregationOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    setupScope->addStatement(windowOperatorStatement.copy());

    // getWindowDefinition
    auto windowDefDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowDefinition");
    auto getWindowDefinitionCall = call("getWindowDefinition");
    auto windowDefinitionStatement = VarDeclStatement(windowDefDeclaration)
                                         .assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getWindowDefinitionCall));
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    // create struct for entry
    // consists of keys and aggregation values
    StructDeclaration partialAggregationEntry = generatePartialAggregationEntry(window, windowAggregation);
    context->code->structDeclarationInputTuples.push_back(partialAggregationEntry);

    uint64_t aggValueSize = getAggregationValueSize(window);
    auto constantValueSize = Constant(tf->createValueType(DataTypeFactory::createBasicValue(aggValueSize)));

    auto setupWindowHandler = call("setup");
    setupWindowHandler->addParameter(VarRef(context->code->varDeclarationExecutionContext));
    setupWindowHandler->addParameter(constantValueSize);
    setupScope->addStatement(VarRef(windowOperatorHandlerDeclaration).accessPtr(setupWindowHandler).createCopy());
    return 0;
}

uint64_t CCodeGenerator::generateInferModelSetup(PipelineContextPtr context,
                                                 InferModel::InferModelOperatorHandlerPtr operatorHandler) {

    context->registerOperatorHandler(operatorHandler);
    return 0;
}

uint64_t CCodeGenerator::generateWindowSetup(Windowing::LogicalWindowDefinitionPtr window,
                                             SchemaPtr,
                                             PipelineContextPtr context,
                                             uint64_t id,
                                             Windowing::WindowOperatorHandlerPtr windowOperatorHandler) {
    auto tf = getTypeFactory();
    auto idParam = VariableDeclaration::create(tf->createAnonymusDataType("auto"), std::to_string(id));

    auto executionContextRef = VarRefStatement(context->code->varDeclarationExecutionContext);
    auto windowOperatorIndex = context->registerOperatorHandler(windowOperatorHandler);

    // create a new setup scope for this operator
    auto setupScope = context->createSetupScope();

    auto windowOperatorHandlerDeclaration =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowOperatorHandler");
    auto getOperatorHandlerCall = call("getOperatorHandler<Windowing::WindowOperatorHandler>");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(windowOperatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);

    auto windowOperatorStatement =
        VarDeclStatement(windowOperatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
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
    auto resultSchemaStatement =
        VarDeclStatement(resultSchemaDeclaration).assign(VarRef(windowOperatorHandlerDeclaration).accessPtr(getResultSchemaCall));
    setupScope->addStatement(resultSchemaStatement.copy());

    auto keyStamp = window->isKeyed() ? window->getKeys()[0]->getStamp() : window->getWindowAggregation()[0]->on()->getStamp();
    auto keyType = tf->createDataType(keyStamp);

    auto aggregation = window->getWindowAggregation();
    auto aggregationInputType = tf->createDataType(aggregation[0]->getInputStamp());

    // If aggregation is AVG, initiate an AVGPartialType
    std::string partialAggregateTypeCode;
    if (aggregation[0]->getType() == Windowing::WindowAggregationDescriptor::Type::Avg) {
        partialAggregateTypeCode = "Windowing::AVGPartialType<" + aggregationInputType->getCode()->code_ + ">";
    } else if (aggregation[0]->getType() == Windowing::WindowAggregationDescriptor::Type::Median) {
        partialAggregateTypeCode = "std::vector<" + aggregationInputType->getCode()->code_ + ">";
    } else {
        // otherwise, get the code directly from the partialAggregateStamp
        auto partialAggregateType = tf->createDataType(aggregation[0]->getPartialAggregateStamp());
        partialAggregateTypeCode = partialAggregateType->getCode()->code_;
    }

    auto partialAggregateInitialValue =
        VariableDeclaration::create(tf->createAnonymusDataType("auto"), "partialAggregateInitialValue");
    auto finalAggregateType = tf->createDataType(aggregation[0]->getFinalAggregateStamp());
    auto executableAggregation = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "aggregation");

    // generate code for aggregation initialization
    generateCodeForAggregationInitialization(setupScope,
                                             executableAggregation,
                                             partialAggregateInitialValue,
                                             aggregationInputType,
                                             aggregation[0]);

    auto policy = window->getTriggerPolicy();
    auto executableTrigger = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "trigger");
    if (policy->getPolicyType() == Windowing::TriggerType::triggerOnTime) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnTimeTriggerPolicy::create");
        auto constantTriggerTime =
            Constant(tf->createValueType(DataTypeFactory::createBasicValue(triggerDesc->getTriggerTimeInMs())));
        createTriggerCall->addParameter(constantTriggerTime);
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
        NES_WARNING("This mode is not supported anymore");
    } else if (policy->getPolicyType() == Windowing::TriggerType::triggerOnWatermarkChange) {
        auto triggerDesc = std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(policy);
        auto createTriggerCall = call("Windowing::ExecutableOnWatermarkChangeTriggerPolicy::create");
        auto triggerStatement = VarDeclStatement(executableTrigger).assign(createTriggerCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode={} not implemented",
                        std::string(magic_enum::enum_name(policy->getPolicyType())));
    }

    auto action = window->getTriggerAction();
    auto executableTriggerAction = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "triggerAction");
    if (action->getActionType() == Windowing::ActionType::WindowAggregationTriggerAction) {
        auto createTriggerActionCall = call("Windowing::ExecutableCompleteAggregationTriggerAction<" + keyType->getCode()->code_
                                            + "," + aggregationInputType->getCode()->code_ + "," + partialAggregateTypeCode + ","
                                            + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        createTriggerActionCall->addParameter(VarRef(resultSchemaDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        createTriggerActionCall->addParameter(VarRef(partialAggregateInitialValue));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else if (action->getActionType() == Windowing::ActionType::SliceAggregationTriggerAction) {
        auto createTriggerActionCall = call("Windowing::ExecutableSliceAggregationTriggerAction<" + keyType->getCode()->code_
                                            + "," + aggregationInputType->getCode()->code_ + "," + partialAggregateTypeCode + ","
                                            + finalAggregateType->getCode()->code_ + ">::create");
        createTriggerActionCall->addParameter(VarRef(windowDefDeclaration));
        createTriggerActionCall->addParameter(VarRef(executableAggregation));
        createTriggerActionCall->addParameter(VarRef(resultSchemaDeclaration));
        createTriggerActionCall->addParameter(VarRef(idParam));
        auto triggerStatement = VarDeclStatement(executableTriggerAction).assign(createTriggerActionCall);
        setupScope->addStatement(triggerStatement.copy());
    } else {
        NES_FATAL_ERROR("Aggregation Handler: mode={} not implemented",
                        std::string(magic_enum::enum_name(action->getActionType())));
    }

    // AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
    //    windowDefinition, executableWindowAggregation, executablePolicyTrigger, executableWindowAction);
    auto windowHandler = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "windowHandler");
    auto createAggregationWindowHandlerCall =
        call("Windowing::AggregationWindowHandler<" + keyType->getCode()->code_ + "," + aggregationInputType->getCode()->code_
             + "," + partialAggregateTypeCode + "," + finalAggregateType->getCode()->code_ + ">::create");
    createAggregationWindowHandlerCall->addParameter(VarRef(windowDefDeclaration));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableAggregation));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTrigger));
    createAggregationWindowHandlerCall->addParameter(VarRef(executableTriggerAction));
    createAggregationWindowHandlerCall->addParameter(VarRef(idParam));
    createAggregationWindowHandlerCall->addParameter(VarRef(partialAggregateInitialValue));

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

    for (const auto& setupScope : context->getSetupScopes()) {
        setupFunction->addStatement(setupScope);
    }

    setupFunction->addStatement(ReturnStatement::create(
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0))))
            .createCopy()));

    auto startFunction = FunctionDefinition::create("start")
                             ->addParameter(code->varDeclarationExecutionContext)
                             ->returns(tf->createDataType(DataTypeFactory::createUInt32()));

    for (const auto& startScope : context->getStartScopes()) {
        startFunction->addStatement(startScope);
    }

    startFunction->addStatement(ReturnStatement::create(
        Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0))))
            .createCopy()));

    auto functionBuilder = FunctionDefinition::create("execute")
                               ->returns(tf->createAnonymusDataType("ExecutionResult"))
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
    for (auto& decl : code->structDeclarationInputTuples) {
        fileBuilder.addDeclaration(decl.copy());
    }

    /* add generic declarations by operators*/
    for (auto& typeDeclaration : code->typeDeclarations) {
        fileBuilder.addDeclaration(typeDeclaration.copy());
    }

    // define param to use in the ctor of pipeline to determine its arity.
    ExpressionStatementPtr arityStatement;
    switch (context->arity) {
        case PipelineContext::PipelineContextArity::Unary: {
            arityStatement = std::make_shared<ConstantExpressionStatement>(tf->createValueType(
                DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "PipelineStageArity::Unary")));
            break;
        }
        case PipelineContext::PipelineContextArity::BinaryLeft: {
            arityStatement = std::make_shared<ConstantExpressionStatement>(tf->createValueType(
                DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "PipelineStageArity::BinaryLeft")));
            break;
        }
        case PipelineContext::PipelineContextArity::BinaryRight: {
            arityStatement = std::make_shared<ConstantExpressionStatement>(tf->createValueType(
                DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "PipelineStageArity::BinaryRight")));
            break;
        }
    }

    auto ctorFunction = ConstructorDefinition::create("ExecutablePipelineStage" + context->pipelineName, true)
                            ->addInitializer("Runtime::Execution::ExecutablePipelineStage", arityStatement);

    auto executablePipeline = ClassDefinition::create("ExecutablePipelineStage" + context->pipelineName);
    executablePipeline->addBaseClass("Runtime::Execution::ExecutablePipelineStage");
    executablePipeline->addMethod(ClassDefinition::Visibility::Public, functionBuilder);
    executablePipeline->addMethod(ClassDefinition::Visibility::Public, setupFunction);
    executablePipeline->addConstructor(ctorFunction);

    auto executablePipelineDeclaration = executablePipeline->getDeclaration();
    auto pipelineNamespace = NamespaceDefinition::create("NES");
    pipelineNamespace->addDeclaration(executablePipelineDeclaration);

    auto createFunction = FunctionDefinition::create("create");

    auto returnStatement = ReturnStatement::create(SharedPointerGen::makeShared(executablePipelineDeclaration->getType()));
    createFunction->addStatement(returnStatement);

    createFunction->returns(SharedPointerGen::createSharedPtrType(
        NES::QueryCompilation::GeneratableTypesFactory::createAnonymusDataType("Runtime::Execution::ExecutablePipelineStage")));
    pipelineNamespace->addDeclaration(createFunction->getDeclaration());
    CodeFile file = fileBuilder.addDeclaration(pipelineNamespace->getDeclaration()).build();

    return file.code;
}

Runtime::Execution::ExecutablePipelineStagePtr
CCodeGenerator::compile(Compiler::JITCompilerPtr jitCompiler,
                        PipelineContextPtr code,
                        QueryCompilerOptions::CompilationStrategy compilationStrategy) {
    std::string src = generateCode(code);
    auto sourceCode = std::make_unique<Compiler::SourceCode>(Compiler::Language::CPP, src);
    auto enableDebugCompilation = compilationStrategy == CompilationStrategy::DEBUG;
    auto enableOptimizations = compilationStrategy == CompilationStrategy::OPTIMIZE;
    auto request = Compiler::CompilationRequest::create(std::move(sourceCode),
                                                        "query",
                                                        false,
                                                        false,
                                                        enableOptimizations,
                                                        enableDebugCompilation);
    auto result = jitCompiler->compile(std::move(request)).share();

    auto futureCompiledExecutablePipelineStage = std::async(std::launch::async, [result, code, src]() {
        auto compiledCode = result.get().getDynamicObject();
        PipelineStageArity const arity = [&ari = code->arity]() {
            switch (ari) {
                case PipelineContext::PipelineContextArity::Unary: return PipelineStageArity::Unary;
                case PipelineContext::PipelineContextArity::BinaryLeft: return PipelineStageArity::BinaryLeft;
                case PipelineContext::PipelineContextArity::BinaryRight: return PipelineStageArity::BinaryRight;
                default: NES_FATAL_ERROR("Unknown PipelineContext. Terminate.");
            }
        }();
        return CompiledExecutablePipelineStage::create(compiledCode, arity, src);
    });
    // defer compilation till the first invocation of the pipeline
    return LazyCompiledExecutablePipelineStage::create(futureCompiledExecutablePipelineStage.share());
}

BinaryOperatorStatement CCodeGenerator::allocateTupleBuffer(const VariableDeclaration& workerContextVariable) {
    auto allocateTupleBuffer = FunctionCallStatement("allocateTupleBuffer");
    return VarRef(workerContextVariable).accessRef(allocateTupleBuffer);
}

BinaryOperatorStatement CCodeGenerator::getBufferSize(const VariableDeclaration& tupleBufferVariable) {
    auto getBufferSizeFunctionCall = FunctionCallStatement("getBufferSize");
    return VarRef(tupleBufferVariable).accessRef(getBufferSizeFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setNumberOfTuples(const VariableDeclaration& tupleBufferVariable,
                                                          const VariableDeclaration& numberOfResultTuples) {
    auto setNumberOfTupleFunctionCall = FunctionCallStatement("setNumberOfTuples");
    setNumberOfTupleFunctionCall.addParameter(VarRef(numberOfResultTuples));
    /* set number of output tuples to result buffer */
    return VarRef(tupleBufferVariable).accessRef(setNumberOfTupleFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setWatermark(const VariableDeclaration& tupleBufferVariable,
                                                     const VariableDeclaration& inputBufferVariable) {
    auto setWatermarkFunctionCall = FunctionCallStatement("setWatermark");
    setWatermarkFunctionCall.addParameter(getWatermark(inputBufferVariable));
    /* copy watermark */
    return VarRef(tupleBufferVariable).accessRef(setWatermarkFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setOriginId(const VariableDeclaration& tupleBufferVariable,
                                                    const VariableDeclaration& inputBufferVariable) {
    auto setOriginIdFunctionCall = FunctionCallStatement("setOriginId");
    setOriginIdFunctionCall.addParameter(getOriginId(inputBufferVariable));
    /* copy watermark */
    return VarRef(tupleBufferVariable).accessRef(setOriginIdFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::setSequenceNumber(VariableDeclaration tupleBufferVariable,
                                                          VariableDeclaration inputBufferVariable) {
    auto setOriginIdFunctionCall = FunctionCallStatement("setSequenceNumber");
    setOriginIdFunctionCall.addParameter(getSequenceNumber(inputBufferVariable));
    /* copy watermark */
    return VarRef(tupleBufferVariable).accessRef(setOriginIdFunctionCall);
}

CCodeGenerator::~CCodeGenerator() = default;
;

BinaryOperatorStatement CCodeGenerator::emitTupleBuffer(const VariableDeclaration& pipelineContext,
                                                        const VariableDeclaration& tupleBufferVariable,
                                                        const VariableDeclaration& workerContextVariable) {
    auto emitTupleBuffer = FunctionCallStatement("emitBuffer");
    emitTupleBuffer.addParameter(VarRef(tupleBufferVariable));
    emitTupleBuffer.addParameter(VarRef(workerContextVariable));
    return VarRef(pipelineContext).accessRef(emitTupleBuffer);
}
BinaryOperatorStatement CCodeGenerator::getBuffer(const VariableDeclaration& tupleBufferVariable) {
    auto getBufferFunctionCall = FunctionCallStatement("getBuffer");
    return VarRef(tupleBufferVariable).accessRef(getBufferFunctionCall);
}
BinaryOperatorStatement CCodeGenerator::getWatermark(const VariableDeclaration& tupleBufferVariable) {
    auto getWatermarkFunctionCall = FunctionCallStatement("getWatermark");
    return VarRef(tupleBufferVariable).accessRef(getWatermarkFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::getOriginId(const VariableDeclaration& tupleBufferVariable) {
    auto getWatermarkFunctionCall = FunctionCallStatement("getOriginId");
    return VarRef(tupleBufferVariable).accessRef(getWatermarkFunctionCall);
}

BinaryOperatorStatement CCodeGenerator::getSequenceNumber(VariableDeclaration tupleBufferVariable) {
    auto getWatermarkFunctionCall = FunctionCallStatement("getSequenceNumber");
    return VarRef(tupleBufferVariable).accessRef(getWatermarkFunctionCall);
}

BinaryOperatorStatement
CCodeGenerator::getAggregationWindowHandler(const VariableDeclaration& pipelineContextVariable,
                                            DataTypePtr keyType,
                                            const Windowing::WindowAggregationDescriptorPtr& windowAggregationDescriptor) {
    auto tf = getTypeFactory();
    // determine the partialAggregate parameter based on the aggregation type
    // Avg aggregation uses AVGPartialType, other aggregates use their getPartialAggregateStamp
    std::string partialAggregateCode;
    if (windowAggregationDescriptor->getType() == Windowing::WindowAggregationDescriptor::Type::Avg) {
        // generated code : Windowing::AVGPartialType<T>
        partialAggregateCode = "Windowing::AVGPartialType<" + TO_CODE(windowAggregationDescriptor->getInputStamp()) + ">";
    } else if (windowAggregationDescriptor->getType() == Windowing::WindowAggregationDescriptor::Type::Median) {
        // generated code : std::vector<T>
        partialAggregateCode = "std::vector<" + TO_CODE(windowAggregationDescriptor->getInputStamp()) + ">";
    } else {
        // generated code : T
        auto partialAggregateType = tf->createDataType(windowAggregationDescriptor->getPartialAggregateStamp());
        partialAggregateCode = partialAggregateType->getCode()->code_;
    }
    // Let K= Key Type, T= Input Type, Partial<T>= Partial Aggregate Type (from above if else), F= Final type
    // generated code : getWindowHandler<NES::Windowing::AggregationWindowHandler, K, Partial<T>, F>
    auto call =
        FunctionCallStatement(std::string("getWindowHandler<NES::Windowing::AggregationWindowHandler, ") + TO_CODE(keyType) + ", "
                              + TO_CODE(windowAggregationDescriptor->getInputStamp()) + "," + partialAggregateCode + ","
                              + TO_CODE(windowAggregationDescriptor->getFinalAggregateStamp()) + " >");
    return VarRef(pipelineContextVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getJoinWindowHandler(const VariableDeclaration& pipelineContextVariable,
                                                             DataTypePtr keyType,
                                                             const std::string& leftType,
                                                             const std::string& rightType) {

    auto tf = getTypeFactory();
    auto call = FunctionCallStatement(std::string("getJoinHandler<NES::Join::JoinHandler, ") + TO_CODE(keyType) + "," + leftType
                                      + "," + rightType + " >");
    return VarRef(pipelineContextVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getBatchJoinHandler(const VariableDeclaration& pipelineContextVariable,
                                                            DataTypePtr keyType,
                                                            const std::string& buildType) {

    auto tf = getTypeFactory();
    auto call = FunctionCallStatement(std::string("getBatchJoinHandler<NES::Join::Experimental::BatchJoinHandler, ")
                                      + TO_CODE(keyType) + "," + buildType + " >");
    return VarRef(pipelineContextVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getStateVariable(const VariableDeclaration& windowHandlerVariable) {
    auto call = FunctionCallStatement("getTypedWindowState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getLeftJoinState(const VariableDeclaration& windowHandlerVariable) {
    auto call = FunctionCallStatement("getLeftJoinState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getRightJoinState(const VariableDeclaration& windowHandlerVariable) {
    auto call = FunctionCallStatement("getRightJoinState");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

BinaryOperatorStatement CCodeGenerator::getWindowManager(const VariableDeclaration& windowHandlerVariable) {
    auto call = FunctionCallStatement("getWindowManager");
    return VarRef(windowHandlerVariable).accessPtr(call);
}

TypeCastExprStatement CCodeGenerator::getTypedBuffer(const VariableDeclaration& tupleBufferVariable,
                                                     const StructDeclaration& structDeclaration) {
    auto tf = getTypeFactory();
    return TypeCast(getBuffer(tupleBufferVariable), tf->createPointer(tf->createUserDefinedType(structDeclaration)));
}

VariableDeclaration CCodeGenerator::getOperatorHandler(const PipelineContextPtr& context,
                                                       const VariableDeclaration& tupleBufferVariable,
                                                       uint64_t operatorIndex,
                                                       NES::Runtime::Execution::OperatorHandlerType type) {
    std::string typeString, identifier;
    switch (type) {
        case NES::Runtime::Execution::OperatorHandlerType::WINDOW:
            typeString = "Windowing::WindowOperatorHandler";
            identifier = "windowOperatorHandler";
            break;
        case Runtime::Execution::OperatorHandlerType::CEP:
            typeString = "NES::CEP::CEPOperatorHandler";
            identifier = "CEPOperatorHandler";
            break;
        case Runtime::Execution::OperatorHandlerType::JOIN:
            typeString = "Join::JoinOperatorHandler";
            identifier = "joinOperatorHandler";
            break;
        case Runtime::Execution::OperatorHandlerType::BATCH_JOIN:
            typeString = "Join::Experimental::BatchJoinOperatorHandler";
            identifier = "batchJoinOperatorHandler";
            break;
        case Runtime::Execution::OperatorHandlerType::KEY_EVENT_TIME_WINDOW:// afaik nothing uses or tests this behaviour
            typeString = "Windowing::Experimental::BatchJoinOperatorHandler";
            identifier = "keyEventTimeWindowOperatorHandler";
            break;
    }

    auto tf = getTypeFactory();
    auto executionContextRef = VarRefStatement(tupleBufferVariable);
    auto operatorHandlerDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), identifier);
    auto getOperatorHandlerCall = call("getOperatorHandler<" + typeString + ">");
    auto constantOperatorHandlerIndex = Constant(tf->createValueType(DataTypeFactory::createBasicValue(operatorIndex)));
    getOperatorHandlerCall->addParameter(constantOperatorHandlerIndex);
    auto operatorStatement =
        VarDeclStatement(operatorHandlerDeclaration).assign(executionContextRef.accessRef(getOperatorHandlerCall));
    context->code->variableInitStmts.push_back(operatorStatement.copy());

    return operatorHandlerDeclaration;
}

VariableDeclaration CCodeGenerator::getWindowOperatorHandler(const PipelineContextPtr& context,
                                                             const VariableDeclaration& tupleBufferVariable,
                                                             uint64_t operatorIndex) {
    return getOperatorHandler(context, tupleBufferVariable, operatorIndex, NES::Runtime::Execution::OperatorHandlerType::WINDOW);
}

VariableDeclaration CCodeGenerator::getCEPIterationOperatorHandler(const PipelineContextPtr& context,
                                                                   const VariableDeclaration& tupleBufferVariable,
                                                                   uint64_t operatorIndex) {
    return getOperatorHandler(context, tupleBufferVariable, operatorIndex, NES::Runtime::Execution::OperatorHandlerType::CEP);
}

VariableDeclaration CCodeGenerator::getJoinOperatorHandler(const PipelineContextPtr& context,
                                                           const VariableDeclaration& tupleBufferVariable,
                                                           uint64_t operatorIndex) {
    return getOperatorHandler(context, tupleBufferVariable, operatorIndex, NES::Runtime::Execution::OperatorHandlerType::JOIN);
}

VariableDeclaration CCodeGenerator::getBatchJoinOperatorHandler(const PipelineContextPtr& context,
                                                                const VariableDeclaration& tupleBufferVariable,
                                                                uint64_t operatorIndex) {
    return getOperatorHandler(context,
                              tupleBufferVariable,
                              operatorIndex,
                              NES::Runtime::Execution::OperatorHandlerType::BATCH_JOIN);
}

}// namespace NES::QueryCompilation
