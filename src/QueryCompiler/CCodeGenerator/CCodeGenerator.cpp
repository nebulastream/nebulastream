#include <API/Schema.hpp>
#include <API/Types/DataTypes.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/DataTypes/AnonymousUserDefinedDataType.hpp>
#include <QueryCompiler/DataTypes/UserDefinedDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <SourceSink/DataSink.hpp>
#include <Util/Logger.hpp>

namespace NES {

CCodeGenerator::CCodeGenerator() : CodeGenerator() {}

const StructDeclaration getStructDeclarationFromSchema(const std::string structName, SchemaPtr schema) {
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

const StructDeclaration getStructDeclarationInputTuple(SchemaPtr schema) {
    return getStructDeclarationFromSchema("InputTuple", schema);
}

const StructDeclaration getStructDeclarationResultTuple(SchemaPtr schema) {
    return getStructDeclarationFromSchema("ResultTuple", schema);
}

const VariableDeclarationPtr getVariableDeclarationForField(const StructDeclaration& structDeclaration,
                                                            const AttributeFieldPtr field) {
    if (structDeclaration.getField(field->name))
        return std::make_shared<VariableDeclaration>(structDeclaration.getVariableDeclaration(field->name));
    else {
        return VariableDeclarationPtr();
    }
}

const std::string toString(void* value, DataTypePtr type) {
    //     if(type->)
    return "";
}

CodeGeneratorPtr CCodeGenerator::create() {
    return std::make_shared<CCodeGenerator>();
}

bool CCodeGenerator::generateCode(SchemaPtr schema, const PipelineContextPtr& context, std::ostream& out) {

    context->inputSchema = schema->copy();
    auto code = context->code;
    code->structDeclaratonInputTuple = getStructDeclarationInputTuple(context->inputSchema);

    /** === set the result tuple depending on the input tuple===*/
    context->resultSchema = context->inputSchema;
    code->structDeclarationResultTuple = getStructDeclarationResultTuple(context->resultSchema);

    /* === declarations === */
    auto tupleBufferType = createAnonymUserDefinedType("NES::TupleBuffer");
    auto pipelineExecutionContextType = createAnonymUserDefinedType("NES::PipelineExecutionContext");
    VariableDeclaration varDeclarationInputBuffer = VariableDeclaration::create(
        createReferenceDataType(tupleBufferType), "inputTupleBuffer");
    VariableDeclaration varDeclarationPipelineExecutionContext = VariableDeclaration::create(
        createReferenceDataType(pipelineExecutionContextType), "pipelineExecutionContext");
    VariableDeclaration varDeclarationState =
        VariableDeclaration::create(createPointerDataType(createAnonymUserDefinedType("void")), "state_var");
    VariableDeclaration varDeclarationWindowManager =
        VariableDeclaration::create(createPointerDataType(createAnonymUserDefinedType("NES::WindowManager")),
                                    "windowManager");
    auto varDeclarationResultBuffer = VariableDeclaration::create(tupleBufferType, "resultTupleBuffer");
    code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    code->varDeclarationExecutionContext = varDeclarationPipelineExecutionContext;
    code->varDeclarationResultBuffer = varDeclarationResultBuffer;
    code->varDeclarationState = varDeclarationState;
    code->varDeclarationWindowManager = varDeclarationWindowManager;

    /* declaration of record index; */
    code->varDeclarationRecordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(UINT64)), "recordIndex",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());
    /* int32_t ret = 0; */
    code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(INT32)), "ret",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());

    code->varDeclarationInputTuples = VariableDeclaration::create(
        createPointerDataType(
            createUserDefinedType(code->structDeclaratonInputTuple)),
        "inputTuples");
    ;

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
bool CCodeGenerator::generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) {

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
bool CCodeGenerator::generateCode(const AttributeFieldPtr field,
                                  const PredicatePtr& pred,
                                  const NES::PipelineContextPtr& context,
                                  std::ostream& out) {

    auto code = context->code;
    auto varDeclarationResultTuples = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(code->structDeclarationResultTuple)), "resultTuples");

    auto declaredMapVar = getVariableDeclarationForField(code->structDeclarationResultTuple, field);
    if (!declaredMapVar) {
        context->resultSchema->addField(field);
        context->code->structDeclarationResultTuple.addField(VariableDeclaration::create(field->data_type,
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

bool CCodeGenerator::generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) {
    NES_DEBUG("CCodeGenerator: Generate code for Sink.");
    auto code = context->code;
    // set result schema to context
    // todo replace to result schema of last operator instead of sink
    context->resultSchema = sink->getSchema();
    // generate result tuple struct
    auto structDeclarationResultTuple = getStructDeclarationResultTuple(context->resultSchema);
    // add type declaration for the result tuple
    code->typeDeclarations.push_back(structDeclarationResultTuple);

    auto varDeclResultTuple =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(structDeclarationResultTuple)),
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
    // 2. emit the buffer to the runtime.
    code->cleanupStmts.push_back(emitTupleBuffer(code->varDeclarationExecutionContext,
                                                 code->varDeclarationResultBuffer)
                                     .copy());

    return true;
}

void CCodeGenerator::generateTupleBufferSpaceCheck(const PipelineContextPtr& context,
                                                   VariableDeclaration varDeclResultTuple,
                                                   StructDeclaration structDeclarationResultTuple) {
    NES_DEBUG("CCodeGenerator: Generate code for tuple buffer check.");

    auto code = context->code;

    // calculate of the maximal number of tuples per buffer -> (buffer size / tuple size) - 1
    // int64_t maxTuple = (resultTupleBuffer.getBufferSize() / 39) - 1;
    // 1. get the size of one result tuple
    auto resultTupleSize = context->getResultSchema()->getSchemaSizeInBytes();
    // 2. initialize max tuple
    auto maxTupleDeclaration = VariableDeclaration::create(createDataType(INT64), "maxTuple");
    // 3. create calculation statement
    auto calculateMaxTupleStatement =
        getBufferSize(code->varDeclarationResultBuffer) / Constant(resultTupleSize);
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
    // 1.2 emit the output buffers to the runtime -> pipelineExecutionContext.emitBuffer(resultTupleBuffer);
    thenStatement->addStatement(emitTupleBuffer(code->varDeclarationExecutionContext,
                                                code->varDeclarationResultBuffer)
                                    .copy());
    // 2.1 reset the numberOfResultTuples to 0 -> numberOfResultTuples = 0;
    thenStatement->addStatement(VarRef(code->varDeclarationNumberOfResultTuples).assign(Constant(0)).copy());
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
bool CCodeGenerator::generateCode(const WindowDefinitionPtr& window,
                                  const PipelineContextPtr& context,
                                  std::ostream& out) {
    context->setWindow(window);
    auto constStatement = ConstantExpressionStatement((createBasicTypeValue(BasicType::UINT64, "0")));

    auto stateVariableDeclaration = VariableDeclaration::create(
        createPointerDataType(createAnonymUserDefinedType(
            "NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>")),
        "state_variable");

    auto stateVarDeclarationStatement = VarDeclStatement(stateVariableDeclaration)
                                            .assign(TypeCast(VarRef(context->code->varDeclarationState), stateVariableDeclaration.getDataType()));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        stateVarDeclarationStatement));

    // Read key value from record
    auto keyVariableDeclaration = VariableDeclaration::create(createDataType(BasicType::INT64), "key");
    auto keyVariableAttributeDeclaration =
        context->code->structDeclaratonInputTuple.getVariableDeclaration(window->onKey->name);
    auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration)
                                             .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
                                                 VarRef(
                                                     keyVariableAttributeDeclaration)));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        keyVariableAttributeStatement));

    // get key handle for current key
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(
        createAnonymUserDefinedType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement = VarDeclStatement(keyHandlerVariableDeclaration)
                                           .assign(VarRef(stateVariableDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        keyHandlerVariableStatement));

    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(
        createAnonymUserDefinedType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(ConstantExpressionStatement(INT64, "0"));
    auto windowStateVariableStatement = VarDeclStatement(windowStateVariableDeclaration)
                                            .assign(VarRef(keyHandlerVariableDeclaration).accessRef(getValueFromKeyHandle));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        windowStateVariableStatement));

    // get current timestamp
    // TODO add support for event time
    auto currentTimeVariableDeclaration = VariableDeclaration::create(
        createAnonymUserDefinedType("auto"), "current_ts");
    auto getCurrentTs = FunctionCallStatement("NES::getTsFromClock");
    auto getCurrentTsStatement = VarDeclStatement(currentTimeVariableDeclaration)
                                     .assign(getCurrentTs);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        getCurrentTsStatement));

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(currentTimeVariableDeclaration));
    sliceStream.addParameter(VarRef(windowStateVariableDeclaration));
    auto call =
        std::make_shared<BinaryOperatorStatement>(VarRef(context->code->varDeclarationWindowManager).accessPtr(sliceStream));
    context->code->currentCodeInsertionPoint->addStatement(call);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(currentTimeVariableDeclaration));
    auto getSliceIndexByTsCall = VarRef(windowStateVariableDeclaration).accessPtr(getSliceIndexByTs);
    auto currentSliceIndexVariableDeclaration = VariableDeclaration::create(
        createDataType(BasicType(UINT64)), "current_slice_index");
    auto current_slice_ref = VarRef(currentSliceIndexVariableDeclaration);
    auto currentSliceIndexVariableStatement = VarDeclStatement(currentSliceIndexVariableDeclaration)
                                                  .assign(getSliceIndexByTsCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        currentSliceIndexVariableStatement));

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(windowStateVariableDeclaration).accessPtr(getPartialAggregates);
    VariableDeclaration partialAggregatesVarDeclaration = VariableDeclaration::create(
        createAnonymUserDefinedType("std::vector<int64_t>&"), "partialAggregates");
    auto assignment = VarDeclStatement(partialAggregatesVarDeclaration)
                          .assign(getPartialAggregatesCall);
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(partialAggregatesVarDeclaration)[current_slice_ref];
    window->windowAggregation->compileLiftCombine(
        context->code->currentCodeInsertionPoint,
        partialRef,
        context->code->structDeclaratonInputTuple,
        VarRef(context->code->varDeclarationInputTuples)[VarRefStatement(VarRef(*(context->code->varDeclarationRecordIndex)))]);

    context->code->cleanupStmts.push_back(emitTupleBuffer(context->code->varDeclarationExecutionContext,
                                                          context->code->varDeclarationResultBuffer)
                                              .copy());
    return true;
}

ExecutablePipelinePtr CCodeGenerator::compile(const CompilerArgs&, const GeneratedCodePtr& code) {

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */

    // FunctionDeclaration main_function =
    FunctionBuilder functionBuilder = FunctionBuilder::create("compiled_query")
                                          .returns(createDataType(BasicType(UINT32)))
                                          .addParameter(code->varDeclarationInputBuffer)
                                          .addParameter(code->varDeclarationState)
                                          .addParameter(code->varDeclarationWindowManager)
                                          .addParameter(code->varDeclarationExecutionContext);
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
    Compiler compiler;
    CompiledCodePtr compiledCode = compiler.compile(file.code, true /*debugging flag replace later*/);
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

CCodeGenerator::~CCodeGenerator(){};

BinaryOperatorStatement CCodeGenerator::emitTupleBuffer(VariableDeclaration pipelineContext,
                                                        VariableDeclaration tupleBufferVariable) {
    auto emitTupleBuffer = FunctionCallStatement("emitBuffer");
    emitTupleBuffer.addParameter(VarRef(tupleBufferVariable));
    return VarRef(pipelineContext).accessRef(emitTupleBuffer);
}
BinaryOperatorStatement CCodeGenerator::getBuffer(VariableDeclaration tupleBufferVariable) {
    auto getBufferFunctionCall = FunctionCallStatement("getBuffer");
    return VarRef(tupleBufferVariable).accessRef(getBufferFunctionCall);
}
TypeCastExprStatement CCodeGenerator::getTypedBuffer(VariableDeclaration tupleBufferVariable,
                                                     StructDeclaration structDeclaraton) {
    return TypeCast(getBuffer(tupleBufferVariable), createPointerDataType(createUserDefinedType(structDeclaraton)));
}
}// namespace NES