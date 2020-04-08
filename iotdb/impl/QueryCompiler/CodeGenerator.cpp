
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
    : variable_decls(),
      variable_init_stmts(),
      for_loop_stmt(),
      current_code_insertion_point(),
      cleanup_stmts(),
      return_stmt(),
      var_decl_id(),
      var_decl_return(),
      struct_decl_tuple_buffer(getStructDeclarationTupleBuffer()),
      struct_decl_state(StructDeclaration::create("InputTuple2", "")),
      struct_decl_input_tuple(StructDeclaration::create("InputTuple", "")),
      struct_decl_result_tuple(StructDeclaration::create("ResultTuple", "")),
      var_decl_tuple_buffers(VariableDeclaration::create(createDataType(INT32), "input_buffers")),
      var_declare_window(VariableDeclaration::create(createDataType(INT32), "window")),
      var_decl_tuple_buffer_output(VariableDeclaration::create(createDataType(INT32), "output_buffer")),
      var_decl_state(VariableDeclaration::create(createDataType(INT32), "state")),
      decl_field_num_tuples_struct_tuple_buf(
          VariableDeclaration::create(createDataType(INT32), "decl_field_num_tuples_NOT_DEFINED")),
      decl_field_data_ptr_struct_tuple_buf(
          VariableDeclaration::create(createDataType(INT32), "decl_field_data_ptr_NOT_DEFINED")),
      var_decl_input_tuple(VariableDeclaration::create(createDataType(INT32), "input_tuple_NOT_DEFINED")),
      var_num_for_loop(VariableDeclaration::create(createDataType(UINT64), "num_result_tuples")),
      type_decls() {
}

const PipelineContextPtr createPipelineContext() { return std::make_shared<PipelineContext>(); }

typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

CodeGenerator::CodeGenerator(const CodeGenArgs& args) : args_(args) {}

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
/**
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
}*/

const StructDeclaration getStructDeclarationFromSchema(const std::string struct_name, SchemaPtr schema) {
    /* struct definition for tuples */
    StructDeclaration struct_decl_tuple = StructDeclaration::create(struct_name, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    struct_decl_tuple.makeStructCompact();

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

const VariableDeclarationPtr getVariableDeclarationForField(const StructDeclaration& struct_decl,
                                                            const AttributeFieldPtr field) {
    if (struct_decl.getField(field->name))
        return std::make_shared<VariableDeclaration>(struct_decl.getVariableDeclaration(field->name));
    else {
        return VariableDeclarationPtr();
    }
}

const std::string toString(void* value, DataTypePtr type) {
    //     if(type->)
    return "";
}

std::string toString(TupleBuffer& buffer, SchemaPtr schema) { return toString(&buffer, schema); }

std::string toString(TupleBuffer* buffer, SchemaPtr schema) {
    if (!buffer)
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

    char* buf = (char*) buffer->getBuffer();
    for (uint32_t i = 0;
         i < buffer->getNumberOfTuples()*buffer->getTupleSizeInBytes(); i +=
                                                                            buffer->getTupleSizeInBytes()) {
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

    context->inputSchema = schema->makeDeepCopy();

    StructDeclaration struct_decl_tuple_buffer = getStructDeclarationTupleBuffer();
    StructDeclaration struct_decl_tuple = getStructDeclarationInputTuple(context->inputSchema);

    context->addTypeDeclaration(struct_decl_tuple_buffer);
    context->addTypeDeclaration(struct_decl_tuple);

    context->code->struct_decl_tuple_buffer = struct_decl_tuple_buffer;
    context->code->struct_decl_input_tuple = struct_decl_tuple;

    /** === set the result tuple depending on the input tuple===*/
    context->resultSchema = context->inputSchema;
    StructDeclaration struct_result_tuple = getStructDeclarationResultTuple(context->resultSchema);
    context->code->struct_decl_result_tuple = struct_result_tuple;
    context->addTypeDeclaration(struct_result_tuple);

    /* === declarations === */

    VariableDeclaration var_decl_tuple_buffers = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)),
        "input_buffer");
    VariableDeclaration var_decl_tuple_buffer_output = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "output_tuple_buffer");
    VariableDeclaration var_decl_window =
        VariableDeclaration::create(createPointerDataType(createAnnonymUserDefinedType("void")), "state_var");
    VariableDeclaration var_decl_window_manager =
        VariableDeclaration::create(createPointerDataType(createAnnonymUserDefinedType("NES::WindowManager")),
                                    "window_manager");

    context->code->var_decl_tuple_buffers = var_decl_tuple_buffers;
    context->code->var_decl_tuple_buffer_output = var_decl_tuple_buffer_output;
    context->code->var_decl_state = var_decl_window;
    context->code->var_declare_window = var_decl_window_manager;

    /* Tuple *tuples; */

    VariableDeclaration var_decl_tuple =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_tuple)), "tuples");

    /* TupleBuffer *tuple_buffer_1; */
    VariableDeclaration var_decl_tuple_buffer_1 = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(getStructDeclarationTupleBuffer())), "tuple_buffer_1");

    /* uint64_t id = 0; */
    context->code->var_decl_id = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(UINT64)), "id",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());
    /* int32_t ret = 0; */
    context->code->var_decl_return = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(INT32)), "ret",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());

    context->code->var_decl_input_tuple = var_decl_tuple;
    context->code->variable_decls.push_back(var_decl_tuple);
    context->code->variable_decls.push_back(var_decl_tuple_buffer_1);
    context->code->variable_decls.push_back(*(context->code->var_decl_return.get()));

    /* variable declarations for fields inside structs */
    // VariableDeclaration decl_field_campaign_id = struct_decl_tuple.getVariableDeclaration("campaign_id");
    VariableDeclaration decl_field_num_tuples_struct_tuple_buf =
        struct_decl_tuple_buffer.getVariableDeclaration("num_tuples");
    VariableDeclaration decl_field_data_ptr_struct_tuple_buf = struct_decl_tuple_buffer.getVariableDeclaration("data");

    context->code->decl_field_num_tuples_struct_tuple_buf = decl_field_num_tuples_struct_tuple_buf;
    context->code->decl_field_data_ptr_struct_tuple_buf = decl_field_data_ptr_struct_tuple_buf;

    /** init statements before for loop */

    /* tuple_buffer_1 = window_buffer[0]; */
    context->code->variable_init_stmts.push_back(
        VarRefStatement(var_decl_tuple_buffer_1)
            .assign(VarRefStatement(var_decl_tuple_buffers))
            .copy());
    /*  tuples = (Tuple *)tuple_buffer_1->data;*/
    context->code->variable_init_stmts.push_back(
        VarRef(var_decl_tuple)
            .assign(TypeCast(
                VarRefStatement(var_decl_tuple_buffer_1).accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
                createPointerDataType(createUserDefinedType(struct_decl_tuple))))
            .copy());

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    context->code->for_loop_stmt = std::make_shared<FOR>(
        *(context->code->var_decl_id.get()),
        (VarRef(*(context->code->var_decl_id.get())) <
            (VarRef(var_decl_tuple_buffer_1).accessPtr(VarRef(decl_field_num_tuples_struct_tuple_buf)))),
        ++VarRef(*(context->code->var_decl_id.get())));
    context->code->current_code_insertion_point = context->code->for_loop_stmt->getCompoundStatement();

    context->code->return_stmt = std::make_shared<ReturnStatement>(VarRefStatement(*context->code->var_decl_return));

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

    std::shared_ptr<IF> if_stmt = std::make_shared<IF>(*expr);
    CompoundStatementPtr compound_stmt = if_stmt->getCompoundStatement();
    /* update current compound_stmt*/
    context->code->current_code_insertion_point->addStatement(if_stmt);
    context->code->current_code_insertion_point = compound_stmt;

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

    StructDeclaration
        struct_decl_result_tuple = (getStructDeclarationFromSchema("result_tuples", context->resultSchema));
    VariableDeclaration var_decl_result_tuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_result_tuple)), "result_tuples");

    DeclarationPtr declaredMapVar = getVariableDeclarationForField(context->code->struct_decl_result_tuple, field);
    if (!declaredMapVar) {
        context->resultSchema->addField(field);
        context->code->struct_decl_result_tuple.addField(VariableDeclaration::create(field->data_type, field->name));
        declaredMapVar = getVariableDeclarationForField(context->code->struct_decl_result_tuple, field);
    }
    context->code->override_fields.push_back(declaredMapVar);
    VariableDeclaration var_map_i = context->code->struct_decl_result_tuple.getVariableDeclaration(field->name);

    BinaryOperatorStatement
        callVar = VarRef(var_decl_result_tuple)[VarRef(context->code->var_num_for_loop)].accessRef(VarRef(var_map_i));
    ExpressionStatmentPtr expr = pred->generateCode(context->code);
    BinaryOperatorStatement assignedMap = (callVar).assign(*expr);
    context->code->current_code_insertion_point->addStatement(assignedMap.copy());

    return true;
}

bool CCodeGenerator::generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) {
    context->resultSchema = sink->getSchema();

    StructDeclaration struct_decl_result_tuple = getStructDeclarationResultTuple(context->resultSchema);
    context->addTypeDeclaration(struct_decl_result_tuple);
    context->code->type_decls.push_back(struct_decl_result_tuple);
    VariableDeclaration var_decl_result_tuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_result_tuple)), "result_tuples");
    context->code->variable_decls.push_back(var_decl_result_tuple);

    VariableDeclaration var_decl_num_result_tuples = VariableDeclaration::create(
        createDataType(BasicType(UINT64)), "num_result_tuples", createBasicTypeValue(BasicType(INT64), "0"));
    context->code->var_num_for_loop = var_decl_num_result_tuples;
    context->code->variable_decls.push_back(var_decl_num_result_tuples);

    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    context->code->variable_init_stmts.push_back(
        VarRef(var_decl_result_tuple)
            .assign(TypeCast(VarRef(context->code->var_decl_tuple_buffer_output)
                                 .accessPtr(VarRef(context->code->decl_field_data_ptr_struct_tuple_buf)),
                             createPointerDataType(createUserDefinedType(struct_decl_result_tuple))))
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
                if (context->code->override_fields.at(j)->getIdentifierName().compare(var_decl_input->getIdentifierName())
                    == 0) {
                    override = true;
                    break;
                }
            }
            if (!override) {
                AssignmentStatment as = {var_decl_result_tuple,
                                         *(var_decl),
                                         var_decl_num_result_tuples,
                                         context->code->var_decl_input_tuple,
                                         *(var_decl),
                                         *(context->code->var_decl_id)};
                StatementPtr stmt = var_decl->getDataType()->getStmtCopyAssignment(as);
                context->code->current_code_insertion_point->addStatement(stmt);
            }
        }
        /* */

        // var_decls.push_back(*var_decl);
        // write_result_tuples.push_back(VarRef(var_decl_result_tuple)[VarRef(*(context->code->var_decl_id))].assign(VarRef(var_decl_result_tuple)[VarRef(*(context->code->var_decl_id))]).copy());
    }
    context->code->current_code_insertion_point->addStatement((++VarRef(var_decl_num_result_tuples)).copy());


    /*  TODO: set number of output tuples to result buffer */
    context->code->cleanup_stmts.push_back(
        VarRef(context->code->var_decl_tuple_buffer_output).accessPtr(VarRef(context->code->decl_field_num_tuples_struct_tuple_buf))
            .assign(VarRef(var_decl_num_result_tuples)).copy()
    );
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

    auto state_var = VariableDeclaration::create(
        createPointerDataType(createAnnonymUserDefinedType(
            "NES::StateVariable<int64_t, NES::WindowSliceStore<int64_t>*>")), "state_variable");

    auto state_var_decl = VarDeclStatement(state_var)
        .assign(TypeCast(VarRef(context->code->var_decl_state), state_var.getDataType()));
    context->code->current_code_insertion_point->addStatement(std::make_shared<BinaryOperatorStatement>(state_var_decl));

    // Read key value from record
    auto key_var = VariableDeclaration::create(
        createDataType(BasicType::INT64), "key");
    VariableDeclaration
        key_var_decl_attr = context->code->struct_decl_input_tuple.getVariableDeclaration(window->onKey->name);
    auto key_var_decl = VarDeclStatement(key_var)
        .assign(VarRef(context->code->var_decl_input_tuple)[VarRef(*context->code->var_decl_id)].accessRef(VarRef(
            key_var_decl_attr)));
    context->code->current_code_insertion_point->addStatement(std::make_shared<BinaryOperatorStatement>(key_var_decl));

    // get key handle for current key
    auto key_handle_var = VariableDeclaration::create(
        createAnnonymUserDefinedType("auto"), "key_value_handle");
    auto getKeyStateVariable = FunctionCallStatement("get");
    getKeyStateVariable.addParameter(VarRef(key_var));
    auto key_handle_dec = VarDeclStatement(key_handle_var)
        .assign(VarRef(state_var).accessPtr(getKeyStateVariable));
    context->code->current_code_insertion_point->addStatement(std::make_shared<BinaryOperatorStatement>(key_handle_dec));



    // access window slice state from state variable via key
    auto window_state_var = VariableDeclaration::create(
        createAnnonymUserDefinedType("auto"), "windowState");
    auto getValueFromKeyHandle = FunctionCallStatement("valueOrDefault");
    getValueFromKeyHandle.addParameter(ConstantExprStatement(INT64, "0"));
    auto window_state_decl = VarDeclStatement(window_state_var)
        .assign(VarRef(key_handle_var).accessRef(getValueFromKeyHandle));
    context->code->current_code_insertion_point->addStatement(std::make_shared<BinaryOperatorStatement>(
        window_state_decl));


    // get current timestamp
    // TODO add support for event time
    auto current_time_var = VariableDeclaration::create(
        createAnnonymUserDefinedType("auto"), "current_ts");
    auto getCurrentTs = FunctionCallStatement("NES::getTsFromClock");
    auto getCurrentTsStatement = VarDeclStatement(current_time_var)
        .assign(getCurrentTs);
    context->code->current_code_insertion_point->addStatement(std::make_shared<BinaryOperatorStatement>(
        getCurrentTsStatement));

    // update slices
    auto sliceStream = FunctionCallStatement("sliceStream");
    sliceStream.addParameter(VarRef(current_time_var));
    sliceStream.addParameter(VarRef(window_state_var));
    auto call =
        std::make_shared<BinaryOperatorStatement>(VarRef(context->code->var_declare_window).accessPtr(sliceStream));
    context->code->current_code_insertion_point->addStatement(call);

    // find the slices for a time stamp
    auto getSliceIndexByTs = FunctionCallStatement("getSliceIndexByTs");
    getSliceIndexByTs.addParameter(VarRef(current_time_var));
    auto getSliceIndexByTsCall = VarRef(window_state_var).accessPtr(getSliceIndexByTs);
    auto var_decl_current_slice = VariableDeclaration::create(
        createDataType(BasicType(UINT64)), "current_slice_index");
    auto current_slice_ref = VarRef(var_decl_current_slice);
    auto var_decl_current_slice_stm = VarDeclStatement(var_decl_current_slice)
        .assign(getSliceIndexByTsCall);
    context->code->current_code_insertion_point->addStatement(std::make_shared<BinaryOperatorStatement>(
        var_decl_current_slice_stm));

    // get the partial aggregates
    auto getPartialAggregates = FunctionCallStatement("getPartialAggregates");
    auto getPartialAggregatesCall = VarRef(window_state_var).accessPtr(getPartialAggregates);
    VariableDeclaration var_decl_partial_aggregates = VariableDeclaration::create(
        createAnnonymUserDefinedType("std::vector<int64_t>&"), "partialAggregates");
    auto assignment = VarDeclStatement(var_decl_partial_aggregates)
        .assign(getPartialAggregatesCall);
    context->code->current_code_insertion_point->addStatement(std::make_shared<BinaryOperatorStatement>(assignment));

    // update partial aggregate
    const BinaryOperatorStatement& partialRef = VarRef(var_decl_partial_aggregates)[current_slice_ref];
    window->windowAggregation->compileLiftCombine(
        context->code->current_code_insertion_point,
        partialRef,
        context->code->struct_decl_input_tuple,
        VarRef(context->code->var_decl_input_tuple)[VarRefStatement(VarRef(*(context->code->var_decl_id)))]
    );

    return true;
}

ExecutablePipelinePtr CCodeGenerator::compile(const CompilerArgs&, const GeneratedCodePtr& code) {

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */

    // FunctionDeclaration main_function =
    FunctionBuilder func_builder = FunctionBuilder::create("compiled_query")
        .returns(createDataType(BasicType(UINT32)))
        .addParameter(code->var_decl_tuple_buffers)
        .addParameter(code->var_decl_state)
        .addParameter(code->var_declare_window)
        .addParameter(code->var_decl_tuple_buffer_output);

    for (auto& var_decl : code->variable_decls) {
        func_builder.addVariableDeclaration(var_decl);
    }
    for (auto& var_init : code->variable_init_stmts) {
        func_builder.addStatement(var_init);
    }

    /* here comes the code for the processing loop */
    func_builder.addStatement(code->for_loop_stmt);

    /* add statements executed after the for loop, for example cleanup code */
    for (auto& stmt : code->cleanup_stmts) {
        func_builder.addStatement(stmt);
    }

    /* add return statement */
    func_builder.addStatement(code->return_stmt);

    FileBuilder file_builder = FileBuilder::create("query.cpp");
    /* add core declarations */
    file_builder.addDeclaration(code->struct_decl_tuple_buffer);
    file_builder.addDeclaration(code->struct_decl_state);
    file_builder.addDeclaration(code->struct_decl_input_tuple);
    /* add generic declarations by operators*/
    for (auto& type_decl : code->type_decls) {
        file_builder.addDeclaration(type_decl);
    }

    CodeFile file = file_builder.addDeclaration(func_builder.build()).build();
    Compiler compiler;
    CompiledCodePtr compiled_code = compiler.compile(file.code);
    return createCompiledExecutablePipeline(compiled_code);
}

CCodeGenerator::~CCodeGenerator() {}

CodeGeneratorPtr createCodeGenerator() { return std::make_shared<CCodeGenerator>(CodeGenArgs()); }

} // namespace NES
