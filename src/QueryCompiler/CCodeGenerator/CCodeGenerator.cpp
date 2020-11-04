#include <API/UserAPIExpression.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/Logger.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>

namespace NES {

CCodeGenerator::CCodeGenerator() : CodeGenerator(), compiler(Compiler::create()) {}

StructDeclaration CCodeGenerator::getStructDeclarationFromSchema(std::string structName, SchemaPtr schema) {
    /* struct definition for tuples */
    StructDeclaration structDeclarationTuple = StructDeclaration::create(structName, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();

    NES_DEBUG("Converting Schema: " << schema->toString());
    NES_DEBUG("Define Struct : " << structName);

    for (size_t i = 0; i < schema->getSize(); ++i) {
        structDeclarationTuple.addField(VariableDeclaration::create(schema->get(i)->getDataType(),
                                                                    schema->get(i)->name));
        NES_DEBUG("Field " << i << ": " << schema->get(i)->getDataType()->toString() << " " << schema->get(i)->name);
    }
    return structDeclarationTuple;
}

StructDeclaration CCodeGenerator::getStructDeclarationFromWindow(std::string structName) {
    StructDeclaration structDeclarationTuple = StructDeclaration::create(structName, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();

    SchemaPtr schema = Schema::create()->addField(createField("start", UINT64))->addField(createField("end", UINT64))->addField(createField("key", UINT64))->addField("value", UINT64);

    for (size_t i = 0; i < schema->getSize(); ++i) {
        structDeclarationTuple.addField(VariableDeclaration::create(schema->get(i)->getDataType(),
                                                                    schema->get(i)->name));
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

CodeGeneratorPtr CCodeGenerator::create() {
    return std::make_shared<CCodeGenerator>();
}

bool CCodeGenerator::generateCodeForScan(SchemaPtr schema, PipelineContextPtr context) {

    context->inputSchema = schema->copy();
    auto code = context->code;
    code->structDeclaratonInputTuple = getStructDeclarationFromSchema("InputTuple", schema);

    /** === set the result tuple depending on the input tuple===*/
    context->resultSchema = context->inputSchema;
    code->structDeclarationResultTuple = getStructDeclarationFromSchema("ResultTuple", schema);
    auto tf = getTypeFactory();
    /* === declarations === */
    auto tupleBufferType = tf->createAnonymusDataType("NES::TupleBuffer");
    auto pipelineExecutionContextType = tf->createAnonymusDataType("NES::PipelineExecutionContext");
    auto workerContextType = tf->createAnonymusDataType("NES::WorkerContext");
    VariableDeclaration varDeclarationInputBuffer = VariableDeclaration::create(
        tf->createReference(tupleBufferType), "inputTupleBuffer");

    VariableDeclaration varDeclarationPipelineExecutionContext = VariableDeclaration::create(
        tf->createReference(pipelineExecutionContextType), "pipelineExecutionContext");
    VariableDeclaration varDeclarationWorkerContext = VariableDeclaration::create(
        tf->createReference(workerContextType), "workerContext");

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
        VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt32()),
                                    "ret",
                                    DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), "0"))
            .copy());

    code->varDeclarationInputTuples = VariableDeclaration::create(
        tf->createPointer(tf->createUserDefinedType(code->structDeclaratonInputTuple)),
        "inputTuples");

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
    code->forLoopStmt = std::make_shared<FOR>(
        code->varDeclarationRecordIndex,
        (VarRef(code->varDeclarationRecordIndex) < (numberOfRecords)).copy(),
        (++VarRef(code->varDeclarationRecordIndex)).copy());

    code->currentCodeInsertionPoint = code->forLoopStmt->getCompoundStatement();

    code->returnStmt = ReturnStatement(VarRefStatement(*code->varDeclarationReturnValue)).createCopy();
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
        context->code->structDeclarationResultTuple.addField(VariableDeclaration::create(field->dataType,
                                                                                         field->name));
        declaredMapVar = getVariableDeclarationForField(code->structDeclarationResultTuple, field);
    }
    code->override_fields.push_back(declaredMapVar);
    auto varMapI = code->structDeclarationResultTuple.getVariableDeclaration(field->name);
    auto callVar = VarRef(varDeclarationResultTuples)[VarRef(code->varDeclarationNumberOfResultTuples)]
                       .accessRef(VarRef(varMapI));
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
        VariableDeclaration::create(tf->createPointer(tf->createUserDefinedType(structDeclarationResultTuple)),
                                    "resultTuples");

    /* ResultTuple* resultTuples = (ResultTuple*)resultTupleBuffer.getBuffer();*/
    code->variableInitStmts.push_back(
        VarDeclStatement(varDeclResultTuple)
            .assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple))
            .copy());

    /**
     * @brief TODO the following code is black magic it need to be refactored in a larger issue.
     */
    std::vector<VariableDeclaration> var_decls;
    std::vector<StatementPtr> write_result_tuples;
    for (size_t i = 0; i < context->resultSchema->getSize(); ++i) {
        auto resultField = context->resultSchema->get(i);
        auto variableDeclaration = getVariableDeclarationForField(structDeclarationResultTuple, resultField);
        if (!variableDeclaration) {
            NES_ERROR("CodeGenerator: Could not extract field " << resultField->toString()
                                                                << " from struct "
                                                                << structDeclarationResultTuple.getTypeName());
            NES_DEBUG("CodeGenerator: W>");
        }
        auto varDeclarationInput = getVariableDeclarationForField(code->structDeclarationResultTuple, resultField);
        if (varDeclarationInput) {
            bool override = false;
            for (size_t j = 0; j < code->override_fields.size(); j++) {
                if (code->override_fields.at(j)->getIdentifierName().compare(varDeclarationInput->getIdentifierName())
                    == 0) {
                    override = true;
                    break;
                }
            }
            if (!override) {
                auto varDeclarationField =
                    getVariableDeclarationForField(structDeclarationResultTuple, resultField);
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
    code->cleanupStmts.push_back(setNumberOfTuples(code->varDeclarationResultBuffer,
                                                   code->varDeclarationNumberOfResultTuples)
                                     .copy());

    // 2. copy watermark
    code->cleanupStmts.push_back(setWatermark(code->varDeclarationResultBuffer,
                                              code->varDeclarationInputBuffer)
                                     .copy());

    // 3. copy origin id
    code->cleanupStmts.push_back(setOriginId(code->varDeclarationResultBuffer,
                                             code->varDeclarationInputBuffer)
                                     .copy());

    // 4. emit the buffer to the runtime.
    code->cleanupStmts.push_back(
        emitTupleBuffer(code->varDeclarationExecutionContext,
                        code->varDeclarationResultBuffer,
                        code->varDeclarationWorkerContext)
            .copy());

    return true;
}

void CCodeGenerator::generateTupleBufferSpaceCheck(PipelineContextPtr context,
                                                   VariableDeclaration varDeclResultTuple,
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
    auto calculateMaxTupleStatement =
        getBufferSize(code->varDeclarationResultBuffer) / Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(resultTupleSize))));
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
        setNumberOfTuples(code->varDeclarationResultBuffer,
                          code->varDeclarationNumberOfResultTuples)
            .copy());

    // 1.1 set the origin id to the output buffer -> resultTupleBuffer.setOriginId(numberOfResultTuples);

    thenStatement->addStatement(
        setOriginId(code->varDeclarationResultBuffer,
                    code->varDeclarationInputBuffer)
            .copy());
    // 1.1 set the watermar to the output buffer -> resultTupleBuffer.setWatermark(numberOfResultTuples);
    thenStatement->addStatement(
        setWatermark(code->varDeclarationResultBuffer,
                     code->varDeclarationInputBuffer)
            .copy());

    // 1.2 emit the output buffers to the runtime -> pipelineExecutionContext.emitBuffer(resultTupleBuffer);
    thenStatement->addStatement(
        emitTupleBuffer(
            code->varDeclarationExecutionContext,
            code->varDeclarationResultBuffer,
            code->varDeclarationWorkerContext)
            .copy());
    // 2.1 reset the numberOfResultTuples to 0 -> numberOfResultTuples = 0;
    thenStatement->addStatement(VarRef(code->varDeclarationNumberOfResultTuples).assign(Constant(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(0))))).copy());
    // 2.2 allocate a new buffer -> resultTupleBuffer = pipelineExecutionContext.allocateTupleBuffer();
    thenStatement->addStatement(VarRef(code->varDeclarationResultBuffer).assign(allocateTupleBuffer(code->varDeclarationExecutionContext)).copy());
    // 2.2 get typed result buffer from resultTupleBuffer -> resultTuples = (ResultTuple*)resultTupleBuffer.getBuffer();
    thenStatement->addStatement(VarRef(varDeclResultTuple).assign(getTypedBuffer(code->varDeclarationResultBuffer, structDeclarationResultTuple)).copy());
}

/**
 * Code Generation for the window operator
 * @param window windowdefinition
 * @param context pipeline context
 * @param out
 * @return
 */
bool CCodeGenerator::generateCodeForCompleteWindow(Windowing::LogicalWindowDefinitionPtr window, PipelineContextPtr context) {
    auto tf = getTypeFactory();
    auto constStatement = ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), "0")));

    if (context->pipelineName != "SlicingWindowType") {
        context->pipelineName = "CompleteWindowType";
    }

    VariableDeclaration var_decl_id = VariableDeclaration::create(
        tf->createDataType(DataTypeFactory::createInt64()),
        context->pipelineName);

    auto windowManagerVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowStateVar");

    auto windowHandlerVariableDeclration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowHandler");

    NES_ASSERT(!window->getOnKey()->getStamp()->isUndefined(), "window on key is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getInputStamp()->isUndefined(), "window input type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getPartialAggregateStamp()->isUndefined(), "window partial type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getFinalAggregateStamp()->isUndefined(), "window final type is undefined");

    auto getWindowHandlerStatement = getWindowHandler(
        context->code->varDeclarationExecutionContext, Aggregation, window->getOnKey()->getStamp(),
        window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
        window->getWindowAggregation()->getFinalAggregateStamp());
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());

    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());

    // Read key value from record
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(DataTypeFactory::createInt64()), "key");
    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration =
            context->code->structDeclaratonInputTuple.getVariableDeclaration(window->getOnKey()->getFieldName());
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration)
                                                 .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                                                     VarRef(
                                                         keyVariableAttributeDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
            keyVariableAttributeStatement));
    } else {
        auto defaultKeyAssignment = VarDeclStatement(keyVariableDeclaration).assign(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
            defaultKeyAssignment));
    }

    // get key handle for current key
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "key_value_handle");

    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement = VarDeclStatement(keyHandlerVariableDeclaration)
                                           .assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        keyHandlerVariableStatement));
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");

    // set the default value for window state
    if (std::dynamic_pointer_cast<Windowing::MinAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "INT64_MAX"))));
    } else if (std::dynamic_pointer_cast<Windowing::MaxAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "INT64_MIN"))));
    } else if (std::dynamic_pointer_cast<Windowing::SumAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
    } else if (std::dynamic_pointer_cast<Windowing::CountAggregationDescriptor>(window->getWindowAggregation()) != nullptr) {
        getValueFromKeyHandle.addParameter(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
    } else {
        NES_FATAL_ERROR("Window Handler: could not cast aggregation type");
    }

    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));

    context->code->currentCodeInsertionPoint
        ->addStatement(std::make_shared<BinaryOperatorStatement>(windowStateVariableStatement));

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (window->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::ProcessingTime) {
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));
    } else {
        auto tsVariableDeclaration =
            context->code->structDeclaratonInputTuple.getVariableDeclaration(window->getWindowType()->getTimeCharacteristic()->getField()->name);
        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration)
                                                  .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                                                      VarRef(tsVariableDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(tsVariableDeclarationStatement));
    }

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    auto call =
        std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration = VariableDeclaration::create(
        tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement = VarDeclStatement(currentSliceIndexVariableDeclaration)
                                                  .assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        currentSliceIndexVariableStatement));

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration)
                          .assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    window->getWindowAggregation()->compileLiftCombine(
        context->code->currentCodeInsertionPoint,
        partialRef,
        context->code->structDeclaratonInputTuple,
        VarRef(context->code->varDeclarationInputTuples)[VarRefStatement(VarRef(*(context->code->varDeclarationRecordIndex)))]);

    NES_DEBUG("CCodeGenerator: Generate code for pipetype" << context->pipelineName << ": "
                                                           << " with code=" << context->code);

    return true;
}

bool CCodeGenerator::generateCodeForSlicingWindow(Windowing::LogicalWindowDefinitionPtr window, PipelineContextPtr context) {
    NES_DEBUG("CCodeGenerator::generateCodeForSlicingWindow with " << window << " pipeline " << context);
    //NOTE: the distinction currently only happens in the trigger
    context->pipelineName = "SlicingWindowType";
    return generateCodeForCompleteWindow(window, context);
}
#if 0
bool CCodeGenerator::generateCodeForJoin(Join::LogicalJoinDefinitionPtr joinDef, PipelineContextPtr context) {
    //    std::cout << joinDef << context << std::endl;
    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for join" << joinDef);

    auto code = context->code;
    /**
     * within the loop
     */
    //        NES::StateVariable<int64_t, NES::Windowing::WindowSliceStore<std::vector<InputTuple>>*>* state_variable = (NES::StateVariable<int64_t, NES::Windowing::WindowSliceStore<std::vector<InputTuple>>*>*) state_var;
    auto windowManagerVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowStateVar");

    auto windowHandlerVariableDeclration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowHandler");

// adapt the code below to work with join descriptor

//
//    NES_ASSERT(!window->getOnKey()->getStamp()->isUndefined(), "window on key is undefined");
//    NES_ASSERT(!window->getWindowAggregation()->getInputStamp()->isUndefined(), "window input type is undefined");
//    NES_ASSERT(!window->getWindowAggregation()->getPartialAggregateStamp()->isUndefined(), "window partial type is undefined");
//    NES_ASSERT(!window->getWindowAggregation()->getFinalAggregateStamp()->isUndefined(), "window final type is undefined");
//
//    auto getWindowHandlerStatement = getWindowHandler(
//        context->code->varDeclarationExecutionContext, window->getOnKey()->getStamp(),
//        window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
//        window->getWindowAggregation()->getFinalAggregateStamp());
//    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());

    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());

    // Read key value from record
    // int64_t key = windowTuples[recordIndex].key;
    //TODO we have to change this depending on the pipeline
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(joinDef->getLeftJoinKey()->getStamp()), joinDef->getLeftJoinKey()->getFieldName());

    auto keyVariableAttributeDeclaration =
        context->code->structDeclaratonInputTuple.getVariableDeclaration(joinDef->getLeftJoinKey()->getFieldName());
    auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration)
                                             .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                                                 VarRef(
                                                     keyVariableAttributeDeclaration)));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        keyVariableAttributeStatement));

    // get key handle for current key
    // auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement = VarDeclStatement(keyHandlerVariableDeclaration)
                                           .assign(VarRef(windowStateVarDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        keyHandlerVariableStatement));

    // access window slice state from state variable via key
    // auto windowState = key_value_handle.value();
    auto windowStateVariableDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("value");
    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        windowStateVariableStatement));

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "current_ts");
    if (joinDef->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::ProcessingTime) {
        //      auto current_ts = NES::Windowing::getTsFromClock();
        auto getCurrentTs = FunctionCallStatement("NES::Windowing::getTsFromClock");
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(getCurrentTs);
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));
    } else {
        //      auto current_ts = inputTuples[recordIndex].time //the time value of the key
        auto tsVariableDeclaration =
            context->code->structDeclaratonInputTuple.getVariableDeclaration(joinDef->getWindowType()->getTimeCharacteristic()->getField()->name);
        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration)
                                                  .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                                                      VarRef(tsVariableDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(tsVariableDeclarationStatement));
    }

    // update slices
    // windowManager->sliceStream(current_ts, windowState);
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    auto call =
        std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    // uint64_t current_slice_index = windowState->getSliceIndexByTs(current_ts);
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration = VariableDeclaration::create(
        tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement = VarDeclStatement(currentSliceIndexVariableDeclaration)
                                                  .assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        currentSliceIndexVariableStatement));

    // get the partial aggregates
    // auto& partialAggregates = windowState->getPartialAggregates();
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration)
                          .assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate with join tuple
    // partialAggregates[current_slice_index].push_back(inputTuples[recordIndex]);
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    auto getPartialAggregatesPushback = FunctionCallStatement("push_back");
    getPartialAggregatesPushback.addParameter(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)]);
    auto statement = partialRef.copy()->accessRef(getPartialAggregatesPushback);
    context->code->currentCodeInsertionPoint->addStatement(statement.copy());

    NES_DEBUG("CCodeGenerator: Generate code for" << context->pipelineName << ": "
                                                  << " with code=" << context->code);
    return true;
}
#endif
bool CCodeGenerator::generateCodeForCombiningWindow(Windowing::LogicalWindowDefinitionPtr window, PipelineContextPtr context) {
    auto tf = getTypeFactory();
    NES_DEBUG("CCodeGenerator: Generate code for combine window " << window);

    auto code = context->code;
    context->pipelineName = "combiningWindowType";
    VariableDeclaration var_decl_id = VariableDeclaration::create(
        tf->createDataType(DataTypeFactory::createInt64()),
        context->pipelineName);

    auto windowManagerVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowManager");

    auto windowStateVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowStateVar");

    auto windowHandlerVariableDeclration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowHandler");

    NES_ASSERT(!window->getOnKey()->getStamp()->isUndefined(), "window on key is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getInputStamp()->isUndefined(), "window input type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getPartialAggregateStamp()->isUndefined(), "window partial type is undefined");
    NES_ASSERT(!window->getWindowAggregation()->getFinalAggregateStamp()->isUndefined(), "window final type is undefined");

    auto getWindowHandlerStatement = getWindowHandler(
        context->code->varDeclarationExecutionContext, Aggregation, window->getOnKey()->getStamp(),
        window->getWindowAggregation()->getInputStamp(), window->getWindowAggregation()->getPartialAggregateStamp(),
        window->getWindowAggregation()->getFinalAggregateStamp());
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowHandlerVariableDeclration).assign(getWindowHandlerStatement).copy());

    auto getWindowManagerStatement = getWindowManager(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowManagerVarDeclaration).assign(getWindowManagerStatement).copy());

    auto getWindowStateStatement = getStateVariable(windowHandlerVariableDeclration);
    context->code->variableInitStmts.emplace_back(VarDeclStatement(windowStateVarDeclaration).assign(getWindowStateStatement).copy());
    // set result schema to context
    // generate result tuple struct

    /**
     * within the loop
     */

    //        NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>* state_variable = (NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>*) state_var;
    auto stateVariableDeclaration = VariableDeclaration::create(
        tf->createPointer(tf->createAnonymusDataType(
            "NES::StateVariable<int64_t, NES::Windowing::WindowSliceStore<int64_t>*>")),
        "state_variable");

    auto stateVarDeclarationStatement = VarDeclStatement(stateVariableDeclaration)
                                            .assign(TypeCast(VarRef(windowStateVarDeclaration), stateVariableDeclaration.getDataType()));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        stateVarDeclarationStatement));

    // Read key value from record
    //        int64_t key = windowTuples[recordIndex].key;
    auto keyVariableDeclaration = VariableDeclaration::create(tf->createDataType(window->getOnKey()->getStamp()), window->getOnKey()->getFieldName());

    if (window->isKeyed()) {
        auto keyVariableAttributeDeclaration =
            context->code->structDeclaratonInputTuple.getVariableDeclaration(window->getOnKey()->getFieldName());
        auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration)
                                                 .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                                                     VarRef(
                                                         keyVariableAttributeDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
            keyVariableAttributeStatement));
    } else {
        auto defaultKeyAssignment = VarDeclStatement(keyVariableDeclaration).assign(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
            defaultKeyAssignment));
    }

    // get key handle for current key
    //        auto key_value_handle = state_variable->get(key);
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement = VarDeclStatement(keyHandlerVariableDeclaration)
                                           .assign(VarRef(stateVariableDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        keyHandlerVariableStatement));

    //    auto windowState = key_value_handle.valueOrDefault(0);
    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(ConstantExpressionStatement(tf->createValueType(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "0"))));
    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        windowStateVariableStatement));

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "start");
    if (window->getWindowType()->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::ProcessingTime) {
        auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration).assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(VarRef(currentTimeVariableDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(getCurrentTsStatement));
    } else {
        currentTimeVariableDeclaration = VariableDeclaration::create(tf->createAnonymusDataType("auto"), "start");
        //        auto tsVariableDeclaration = context->code->structDeclaratonInputTuple.getVariableDeclaration(window->getWindowType()->getTimeCharacteristic()->getField()->name);
        auto tsVariableDeclarationStatement = VarDeclStatement(currentTimeVariableDeclaration)
                                                  .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                                                      VarRef(currentTimeVariableDeclaration)));
        context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(tsVariableDeclarationStatement));
    }

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    auto call =
        std::make_shared<BinaryOperatorStatement>(VarRef(windowManagerVarDeclaration).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration = VariableDeclaration::create(
        tf->createDataType(DataTypeFactory::createUInt64()), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement = VarDeclStatement(currentSliceIndexVariableDeclaration)
                                                  .assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        currentSliceIndexVariableStatement));

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration = VariableDeclaration::create(
        tf->createAnonymusDataType("auto&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration)
                          .assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    window->getWindowAggregation()->compileLiftCombine(
        context->code->currentCodeInsertionPoint,
        partialRef,
        context->code->structDeclaratonInputTuple,
        VarRef(context->code->varDeclarationInputTuples)[VarRefStatement(VarRef(*(context->code->varDeclarationRecordIndex)))]);

    NES_DEBUG("CCodeGenerator: Generate code for" << context->pipelineName << ": "
                                                  << " with code=" << context->code);
    return true;
}

ExecutablePipelinePtr CCodeGenerator::compile(GeneratedCodePtr code) {

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */

    // FunctionDeclaration main_function =
    auto tf = getTypeFactory();
    FunctionBuilder functionBuilder = FunctionBuilder::create("compiled_query")
                                          .returns(tf->createDataType(DataTypeFactory::createUInt32()))
                                          .addParameter(code->varDeclarationInputBuffer)
                                          .addParameter(code->varDeclarationExecutionContext)
                                          .addParameter(code->varDeclarationWorkerContext);
    code->variableDeclarations.push_back(code->varDeclarationNumberOfResultTuples);
    for (auto& variableDeclaration : code->variableDeclarations) {
        functionBuilder.addVariableDeclaration(variableDeclaration);
    }
    for (auto& variableStatement : code->variableInitStmts) {
        functionBuilder.addStatement(variableStatement);
    }

    /* here comes the code for the processing loop */
    functionBuilder.addStatement(code->forLoopStmt);

    /* add statements executed after the for loop, for example cleanup code */
    for (auto& stmt : code->cleanupStmts) {
        functionBuilder.addStatement(stmt);
    }

    /* add return statement */
    functionBuilder.addStatement(code->returnStmt);

    FileBuilder fileBuilder = FileBuilder::create("query.cpp");
    /* add core declarations */
    fileBuilder.addDeclaration(code->structDeclaratonInputTuple);
    /* add generic declarations by operators*/
    for (auto& typeDeclaration : code->typeDeclarations) {
        fileBuilder.addDeclaration(typeDeclaration);
    }

    CodeFile file = fileBuilder.addDeclaration(functionBuilder.build()).build();
    CompiledCodePtr compiledCode = compiler->compile(file.code, true /*debugging flag replace later*/);
    return CompiledExecutablePipeline::create(compiledCode);
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

BinaryOperatorStatement CCodeGenerator::getWindowHandler(VariableDeclaration pipelineContextVariable, WindowCodegenType type, DataTypePtr keyType, DataTypePtr inputType, DataTypePtr partialAggregateType, DataTypePtr finalAggregateType) {
    auto tf = getTypeFactory();
    std::string windowHandlerType = "";
    switch (type) {
        case Aggregation: {
            windowHandlerType = "NES::Windowing::AggregationWindowHandler";
            break;
        }
        case Join: {
            NES_THROW_RUNTIME_ERROR("invalid window handler type");
            break;
        }
        default: NES_THROW_RUNTIME_ERROR("invalid window handler type");
    }
    auto call = FunctionCallStatement(std::string("getWindowHandler< ") + windowHandlerType + ", " + TO_CODE(keyType) + ", " + TO_CODE(inputType) + "," + TO_CODE(partialAggregateType) + "," + TO_CODE(finalAggregateType) + " >");
    return VarRef(pipelineContextVariable).accessRef(call);
}

BinaryOperatorStatement CCodeGenerator::getStateVariable(VariableDeclaration windowHandlerVariable) {
    auto call = FunctionCallStatement("getTypedWindowState");
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
}// namespace NES