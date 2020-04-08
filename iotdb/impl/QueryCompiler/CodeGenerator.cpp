
#include <iostream>
#include <list>

#include <API/Schema.hpp>
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/DataSink.hpp>
#include <Util/Logger.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>

namespace NES {

GeneratedCode::GeneratedCode()
    : variableDeclarations(),
      variableInitStmts(),
      forLoopStmt(),
      currentCodeInsertionPoint(),
      cleanupStmts(),
      returnStmt(),
      varDeclarationRecordIndex(),
      varDeclarationReturnValue(),
      structDeclaratonInputTuple(StructDeclaration::create("InputTuple", "")),
      structDeclarationResultTuple(StructDeclaration::create("ResultTuple", "")),
      varDeclarationInputBuffer(VariableDeclaration::create(createDataType(INT32), "input_buffers")),
      varDeclarationWindowManager(VariableDeclaration::create(createDataType(INT32), "window")),
      varDeclarationResultBuffer(VariableDeclaration::create(createDataType(INT32), "output_buffer")),
      varDeclarationState(VariableDeclaration::create(createDataType(INT32), "state")),
      tupleBufferGetNumberOfTupleCall(FunctionCallStatement("getNumberOfTuples")),
      tupleBufferGetBufferCall(FunctionCallStatement("getBuffer")),
      varDeclarationInputTuples(VariableDeclaration::create(createDataType(INT32), "inputTuples")),
      varDeclarationNumberOfResultTuples(VariableDeclaration::create(
          createDataType(BasicType(UINT64)), "numberOfResultTuples", createBasicTypeValue(BasicType(INT64), "0"))),
      typeDeclarations() {
}

const PipelineContextPtr createPipelineContext() { return std::make_shared<PipelineContext>(); }

typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

CodeGenerator::CodeGenerator(const CodeGenArgs& args) : args(args) {}

CodeGenerator::~CodeGenerator() {}

class CCodeGenerator : public CodeGenerator {
  public:
    CCodeGenerator(const CodeGenArgs& args);
    virtual bool generateCode(SchemaPtr schema, const PipelineContextPtr& context,
                              std::ostream& out) override;
    virtual bool generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) override;
    virtual bool generateCode(const AttributeFieldPtr field,
                              const PredicatePtr& pred,
                              const NES::PipelineContextPtr& context,
                              std::ostream& out) override;
    virtual bool generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) override;
    virtual bool generateCode(const WindowDefinitionPtr& window,
                              const PipelineContextPtr& context,
                              std::ostream& out) override;
    ExecutablePipelinePtr compile(const CompilerArgs&, const GeneratedCodePtr& code) override;
    ~CCodeGenerator() override;
};

CCodeGenerator::CCodeGenerator(const CodeGenArgs& args) : CodeGenerator(args) {}
/**
const StructDeclaration getStructDeclarationTupleBuffer() {
    /** define structure of TupleBuffer
      struct TupleBuffer {
        void *data;
        uint64_t buffer_size;
        uint64_t tuple_size_bytes;
        uint64_t num_tuples;
      };
    */
    StructDeclaration struct_decl_tuple_buffer =
        StructDeclaration::create("TupleBuffer", "")
            .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))), "data"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "buffer_size"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "tuple_size_bytes"))
            .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "num_tuples"));
    return struct_decl_tuple_buffer;
}

const StructDeclaration getStructDeclarationWindowState() {

   define the WindowState struct
 struct WindowState {
  void *windowState;
  };


  StructDeclaration struct_decl_state =
      StructDeclaration::create("WindowSliceStore", "")
          .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))),
                                                "windowState"));
  return struct_decl_state;
}
**/

const StructDeclaration getStructDeclarationFromSchema(const std::string struct_name, const Schema& schema) {
    /* struct definition for tuples */
    StructDeclaration structDeclarationTuple = StructDeclaration::create(structName, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    structDeclarationTuple.makeStructCompact();

    std::cout << "Converting Schema: " << schema->toString() << std::endl;
    std::cout << "Define Struct : " << struct_name << std::endl;

    for (size_t i = 0; i < schema->getSize(); ++i) {
        struct_decl_tuple.addField(VariableDeclaration::create(schema->get(i)->getDataType(), schema->get(i)->name));
        std::cout << "Field " << i << ": " << schema->get(i)->getDataType()->toString() << " " << schema->get(i)->name
                  << std::endl;
    }
    return struct_decl_tuple;
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

std::string toString(TupleBuffer& buffer, const Schema& schema) {
    if (!buffer.isValid())
        return "INVALID_BUFFER_PTR";
    std::stringstream str;
    std::vector<uint32_t> offsets;
    std::vector<DataTypePtr> types;
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        offsets.push_back(schema->get(i)->getFieldSize());
        NES_DEBUG("CodeGenerator: " + std::string("Field Size ") + schema->get(i)->toString() + std::string(": ") +
            std::to_string(schema->get(i)->getFieldSize()));
        types.push_back(schema->get(i)->getDataType());
    }

    uint32_t prefix_sum = 0;
    for (uint32_t i = 0; i < offsets.size(); ++i) {
        uint32_t val = offsets[i];
        offsets[i] = prefix_sum;
        prefix_sum += val;
        NES_DEBUG("CodeGenerator: " + std::string("Prefix Sum: ") + schema->get(i)->toString() + std::string(": ") +
            std::to_string(offsets[i]));
    }

    str << "+----------------------------------------------------+" << std::endl;
    str << "|";
    for (uint32_t i = 0; i < schema->getSize(); ++i) {
        str << schema->get(i)->toString() << "|";
    }
    str << std::endl;
    str << "+----------------------------------------------------+" << std::endl;

    auto buf = buffer.getBufferAs<char>();
    for (uint32_t i = 0; i < buffer.getNumberOfTuples()*buffer.getTupleSizeInBytes();
         i += buffer.getTupleSizeInBytes()) {
        str << "|";
        for (uint32_t s = 0; s < offsets.size(); ++s) {
            void* value = &buf[i + offsets[s]];
            std::string tmp = types[s]->convertRawToString(value);
            str << tmp << "|";
        }
        str << std::endl;
    }
    str << "+----------------------------------------------------+";
    return str.str();
}

bool CCodeGenerator::generateCode(SchemaPtr schema, const PipelineContextPtr& context, std::ostream& out) {

    context->inputSchema = schema->copy();

    StructDeclaration struct_decl_tuple_buffer = getStructDeclarationTupleBuffer();
    StructDeclaration struct_decl_tuple = getStructDeclarationInputTuple(context->inputSchema);


    /** === set the result tuple depending on the input tuple===*/
    context->resultSchema = context->inputSchema;
    context->code->structDeclarationResultTuple = getStructDeclarationResultTuple(context->resultSchema);
    context->addTypeDeclaration(context->code->structDeclarationResultTuple);


    /* === declarations === */
    auto tupleBufferType = createAnonymUserDefinedType("NES::TupleBuffer");
    VariableDeclaration varDeclarationInputBuffer = VariableDeclaration::create(
        createReferenceDataType(tupleBufferType), "inputTupleBuffer");
    VariableDeclaration varDeclarationResultBuffer = VariableDeclaration::create(
        createReferenceDataType(tupleBufferType), "outputTupleBuffer");
    VariableDeclaration varDeclarationState =
        VariableDeclaration::create(createPointerDataType(createAnonymUserDefinedType("void")), "state_var");
    VariableDeclaration varDeclarationWindowManager =
        VariableDeclaration::create(createPointerDataType(createAnonymUserDefinedType("NES::WindowManager")),
                                    "windowManager");

    context->code->varDeclarationInputBuffer = varDeclarationInputBuffer;
    context->code->varDeclarationResultBuffer = varDeclarationResultBuffer;
    context->code->varDeclarationState = varDeclarationState;
    context->code->varDeclarationWindowManager = varDeclarationWindowManager;

    VariableDeclaration varDeclarationInputTuples =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(context->code->structDeclaratonInputTuple)),
                                    "inputTuples");
    /* declaration of record index; */
    context->code->varDeclarationRecordIndex = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(UINT64)), "recordIndex",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());
    /* int32_t ret = 0; */
    context->code->varDeclarationReturnValue = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(INT32)), "ret",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());

    context->code->varDeclarationInputTuples = varDeclarationInputTuples;
    context->code->variableDeclarations.push_back(varDeclarationInputTuples);
    context->code->variableDeclarations.push_back(*(context->code->varDeclarationReturnValue.get()));


    /** init statements before for loop */

    /*  tuples = (InputTuple *)input_buffer.getBuffer()*/
    context->code->variableInitStmts.push_back(
        VarRef(varDeclarationInputTuples)
            .assign(TypeCast(
                VarRefStatement(varDeclarationInputBuffer).accessRef(context->code->tupleBufferGetBufferCall),
                createPointerDataType(createUserDefinedType(context->code->structDeclaratonInputTuple))))
            .copy());

    /* for (uint64_t recordIndex = 0; recordIndex < tuple_buffer_1->num_tuples; ++id) */
    // input_buffer.getNumberOfTuples()
    auto numberOfRecords = VarRef(varDeclarationInputBuffer).accessRef(context->code->tupleBufferGetNumberOfTupleCall);
    context->code->forLoopStmt = std::make_shared<FOR>(
        context->code->varDeclarationRecordIndex,
        (VarRef(context->code->varDeclarationRecordIndex) <
            (numberOfRecords)).copy(),
        (++VarRef(context->code->varDeclarationRecordIndex)).copy());

    context->code->currentCodeInsertionPoint = context->code->forLoopStmt->getCompoundStatement();

    context->code->returnStmt =
        std::make_shared<ReturnStatement>(VarRefStatement(*context->code->varDeclarationReturnValue));

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

    ExpressionStatmentPtr expr = pred->generateCode(context->code);

    std::shared_ptr<IF> ifStmt = std::make_shared<IF>(*expr);
    CompoundStatementPtr compoundStmt = ifStmt->getCompoundStatement();
    /* update current compound_stmt*/
    context->code->currentCodeInsertionPoint->addStatement(ifStmt);
    context->code->currentCodeInsertionPoint = compoundStmt;

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

    VariableDeclaration varDeclarationResultTuples = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(context->code->structDeclarationResultTuple)), "resultTuples");

    DeclarationPtr declaredMapVar = getVariableDeclarationForField(context->code->structDeclarationResultTuple, field);
    if (!declaredMapVar) {
        context->resultSchema->addField(field);
        context->code->struct_decl_result_tuple.addField(VariableDeclaration::create(field->data_type, field->name));
        declaredMapVar = getVariableDeclarationForField(context->code->struct_decl_result_tuple, field);
    }
    context->code->override_fields.push_back(declaredMapVar);
    VariableDeclaration var_map_i = context->code->structDeclarationResultTuple.getVariableDeclaration(field->name);

    BinaryOperatorStatement
        callVar =
        VarRef(varDeclarationResultTuples)[VarRef(context->code->varDeclarationNumberOfResultTuples)].accessRef(VarRef(
            var_map_i));
    ExpressionStatmentPtr expr = pred->generateCode(context->code);
    BinaryOperatorStatement assignedMap = (callVar).assign(*expr);
    context->code->currentCodeInsertionPoint->addStatement(assignedMap.copy());

    return true;
}

bool CCodeGenerator::generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) {
    context->resultSchema = sink->getSchema();

    StructDeclaration structDeclarationResultTuple = getStructDeclarationResultTuple(context->resultSchema);
    context->addTypeDeclaration(structDeclarationResultTuple);
    context->code->typeDeclarations.push_back(structDeclarationResultTuple);
    VariableDeclaration varDeclResultTuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(structDeclarationResultTuple)), "resultTuples");
    context->code->variableDeclarations.push_back(varDeclResultTuple);

    context->code->variableDeclarations.push_back(context->code->varDeclarationNumberOfResultTuples);

    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/

    context->code->variableInitStmts.push_back(
        VarRef(varDeclResultTuple)
            .assign(TypeCast(
                VarRefStatement(context->code->varDeclarationResultBuffer).accessRef(context->code->tupleBufferGetBufferCall),
                createPointerDataType(createUserDefinedType(structDeclarationResultTuple))))
            .copy());

    std::vector<VariableDeclaration> var_decls;
    std::vector<StatementPtr> write_result_tuples;
    for (size_t i = 0; i < context->resultSchema->getSize(); ++i) {
        VariableDeclarationPtr
            var_decl = getVariableDeclarationForField(struct_decl_result_tuple, context->resultSchema->get(i));
        if (!var_decl) {
            NES_ERROR("CodeGenerator: Could not extract field " << context->resultSchema->get(i)->toString() << " from struct "
                                                 << struct_decl_result_tuple.getTypeName());
            NES_DEBUG("CodeGenerator: W>");
        }
        context->code->variable_decls.push_back(*var_decl);

        DeclarationPtr var_decl_input =
            getVariableDeclarationForField(context->code->struct_decl_input_tuple, context->resultSchema->get(i));
        if (var_decl_input) {
            bool override = false;
            for (size_t j = 0; j < context->code->override_fields.size(); j++) {
                if (context->code->override_fields.at(j)->getIdentifierName().compare(varDeclarationInput->getIdentifierName())
                    == 0) {
                    override = true;
                    break;
                }
            }
            if (!override) {
                VariableDeclarationPtr
                    varDeclarationField =
                    getVariableDeclarationForField(structDeclarationResultTuple, context->resultSchema[i]);
                if (!varDeclarationField) {
                    NES_ERROR("Could not extract field " << context->resultSchema[i]->toString() << " from struct "
                                                         << structDeclarationResultTuple.getTypeName());
                    NES_DEBUG("W>");
                }
                context->code->variableDeclarations.push_back(*varDeclarationField);
                AssignmentStatment as = {varDeclResultTuple,
                                         *(varDeclarationField),
                                         context->code->varDeclarationNumberOfResultTuples,
                                         context->code->varDeclarationInputTuples,
                                         *(varDeclarationField),
                                         *(context->code->varDeclarationRecordIndex)};
                StatementPtr stmt = varDeclarationField->getDataType()->getStmtCopyAssignment(as);
                context->code->currentCodeInsertionPoint->addStatement(stmt);
            }
        }
        /* */

        // var_decls.push_back(*var_decl);
        // write_result_tuples.push_back(VarRef(var_decl_result_tuple)[VarRef(*(context->code->var_decl_id))].assign(VarRef(var_decl_result_tuple)[VarRef(*(context->code->var_decl_id))]).copy());
    }
    // increase number of result tuples in loop body.
    context->code->currentCodeInsertionPoint->addStatement((++VarRef(context->code->varDeclarationNumberOfResultTuples)).copy());
    auto setNumberOfTupleFunctionCall = FunctionCallStatement("setNumberOfTuples");
    setNumberOfTupleFunctionCall.addParameter(VarRef(context->code->varDeclarationNumberOfResultTuples));

    /* set number of output tuples to result buffer */
    context->code->cleanupStmts.push_back(
        VarRef(context->code->varDeclarationResultBuffer).accessRef(setNumberOfTupleFunctionCall).copy());

    return true;
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
    auto constStatement = ConstantExprStatement((createBasicTypeValue(BasicType::UINT64, "0")));

    auto stateVariableDeclaration = VariableDeclaration::create(
        createPointerDataType(createAnonymUserDefinedType(
            "NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>")), "state_variable");

    auto stateVarDeclarationStatement = VarDeclStatement(stateVariableDeclaration)
        .assign(TypeCast(VarRef(context->code->varDeclarationState), stateVariableDeclaration.getDataType()));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(
        stateVarDeclarationStatement));

    // Read key value from record
    auto keyVariableDeclaration = VariableDeclaration::create(createDataType(BasicType::INT64), "key");
    auto keyVariableAttributeDeclaration = context->code->structDeclaratonInputTuple.getVariableDeclaration(window->onKey->name);
    auto keyVariableAttributeStatement = VarDeclStatement(keyVariableDeclaration)
        .assign(VarRef(context->code->varDeclarationInputTuples)[VarRef(context->code->varDeclarationRecordIndex)].accessRef(
            VarRef(
                keyVariableAttributeDeclaration)));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(keyVariableAttributeStatement));

    // get key handle for current key
    auto keyHandlerVariableDeclaration = VariableDeclaration::create(
        createAnonymUserDefinedType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(keyVariableDeclaration));
    auto keyHandlerVariableStatement = VarDeclStatement(keyHandlerVariableDeclaration)
        .assign(VarRef(stateVariableDeclaration).accessPtr(getKeyStateVariable));
    context->code->currentCodeInsertionPoint->addStatement(std::make_shared<BinaryOperatorStatement>(keyHandlerVariableStatement));



    // access window slice state from state variable via key
    auto windowStateVariableDeclaration = VariableDeclaration::create(
        createAnonymUserDefinedType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(ConstantExprStatement(INT64, "0"));
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
        std::make_shared<BinaryOperatorStatement>(VarRef(context->code->varDeclarationWindowManager).accessPtr(
            sliceStream));
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
        VarRef(context->code->varDeclarationInputTuples)[VarRefStatement(VarRef(*(context->code->varDeclarationRecordIndex)))]
    );

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
        .addParameter(code->varDeclarationResultBuffer);

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
    CompiledCodePtr compiledCode = compiler.compile(file.code);
    return createCompiledExecutablePipeline(compiledCode);
}

CCodeGenerator::~CCodeGenerator() {}

CodeGeneratorPtr createCodeGenerator() { return std::make_shared<CCodeGenerator>(CodeGenArgs()); }

} // namespace NES
