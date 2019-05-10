
#include <iostream>
#include <list>
#include <sstream>

#include <API/Schema.hpp>
#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>
#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/UnaryOperatorStatement.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Core/DataTypes.hpp>
#include <Runtime/DataSink.hpp>
#include <Util/ErrorHandling.hpp>
#include <API/UserAPIExpression.hpp>

namespace iotdb {

GeneratedCode::GeneratedCode()
        : variable_decls(), variable_init_stmts(), for_loop_stmt(), current_code_insertion_point(), cleanup_stmts(), return_stmt(), var_decl_id(),
          var_decl_return(), struct_decl_tuple_buffer(getStructDeclarationTupleBuffer()),
          struct_decl_state(getStructDeclarationWindowState()),
          struct_decl_input_tuple(StructDeclaration::create("InputTuple", "")),
          struct_decl_result_tuple(StructDeclaration::create("ResultTuple", "")),
          var_decl_tuple_buffers(VariableDeclaration::create(createDataType(INT32), "input_buffers")),
          var_decl_tuple_buffer_output(VariableDeclaration::create(createDataType(INT32), "output_buffer")),
          var_decl_state(VariableDeclaration::create(createDataType(INT32), "state")),
          decl_field_num_tuples_struct_tuple_buf(
              VariableDeclaration::create(createDataType(INT32), "decl_field_num_tuples_NOT_DEFINED")),
          decl_field_data_ptr_struct_tuple_buf(
              VariableDeclaration::create(createDataType(INT32), "decl_field_data_ptr_NOT_DEFINED")),
          var_decl_input_tuple(VariableDeclaration::create(createDataType(INT32), "input_tuple_NOT_DEFINED")),
          type_decls()
    {
    }

class PipelineContext {
  public:
    void addTypeDeclaration(const Declaration&);
    void addVariableDeclaration(const Declaration&);
    //bool hasDeclaration(const Declaration&) const;
    std::vector<DeclarationPtr> type_declarations;
    std::vector<DeclarationPtr> variable_declarations;
};

void PipelineContext::addTypeDeclaration(const Declaration& decl) { type_declarations.push_back(decl.copy()); }
void PipelineContext::addVariableDeclaration(const Declaration& decl) { variable_declarations.push_back(decl.copy()); }

//bool PipelineContext::hasDeclaration(const Declaration& search_decl) const{

////  for(auto& decl : type_declarations){
////      if(decl.isEqual(search_decl)){
////          return true;
////      }
////  }
//  return false;
//}

typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

const PipelineContextPtr createPipelineContext() { return std::make_shared<PipelineContext>(); }


typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

CodeGenerator::CodeGenerator(const CodeGenArgs& args) : args_(args) {}

CodeGenerator::~CodeGenerator() {}

class C_CodeGenerator : public CodeGenerator {
  public:
    C_CodeGenerator(const CodeGenArgs& args);
    virtual bool generateCode(const DataSourcePtr& source, const PipelineContextPtr& context,
                              std::ostream& out) override;
    virtual bool generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) override;
    virtual bool generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) override;
    PipelineStagePtr compile(const CompilerArgs&) override;
    ~C_CodeGenerator() override;

  private:
    GeneratedCode code_;
};

C_CodeGenerator::C_CodeGenerator(const CodeGenArgs& args) : CodeGenerator(args) {}

const StructDeclaration getStructDeclarationTupleBuffer()
{
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

const StructDeclaration getStructDeclarationWindowState()
{
    /**
       define the WindowState struct
     struct WindowState {
      void *window_state;
      };
    */
    StructDeclaration struct_decl_state =
        StructDeclaration::create("WindowState", "")
            .addField(VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))),
                                                  "window_state"));
    return struct_decl_state;
}

const StructDeclaration getStructDeclarationFromSchema(const std::string struct_name, const Schema& schema)
{
    /* struct definition for tuples */
    StructDeclaration struct_decl_tuple = StructDeclaration::create(struct_name, "");
    /* disable padding of bytes to generate compact structs, required for input and output tuple formats */
    struct_decl_tuple.makeStructCompact();

    std::cout << "Converting Schema: " << schema.toString() << std::endl;
    std::cout << "Define Struct : " << struct_name << std::endl;

    for (size_t i = 0; i < schema.getSize(); ++i) {
        struct_decl_tuple.addField(VariableDeclaration::create(schema[i]->getDataType(), schema[i]->name));
        std::cout << "Field " << i << ": " << schema[i]->getDataType()->toString() << " " << schema[i]->name
                  << std::endl;
    }
    //.addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "campaign_id"));
    //.addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "sum"));
    return struct_decl_tuple;
}

const StructDeclaration getStructDeclarationInputTuple(const Schema& schema)
{
    return getStructDeclarationFromSchema("InputTuple", schema);
}

const StructDeclaration getStructDeclarationResultTuple(const Schema& schema)
{
    return getStructDeclarationFromSchema("ResultTuple", schema);
}

const VariableDeclarationPtr getVariableDeclarationForField(const StructDeclaration& struct_decl,
                                                            const AttributeFieldPtr field)
{
    if (struct_decl.getField(field->name))
        return std::make_shared<VariableDeclaration>(struct_decl.getVariableDeclaration(field->name));
    else {
        return VariableDeclarationPtr();
    }
}

const std::string toString(void* value, DataTypePtr type)
{
    //     if(type->)
    return "";
}

std::string toString(const TupleBuffer& buffer, const Schema& schema) { return toString(&buffer, schema); }

std::string toString(const TupleBuffer* buffer, const Schema& schema)
{
    if (!buffer)
        return "INVALID_BUFFER_PTR";
    std::stringstream str;
    std::vector<uint32_t> offsets;
    std::vector<DataTypePtr> types;
    for (uint32_t i = 0; i < schema.getSize(); ++i) {
        offsets.push_back(schema[i]->getFieldSize());
        IOTDB_DEBUG(std::string("Field Size ") + schema[i]->toString() + std::string(": ") +
                    std::to_string(schema[i]->getFieldSize()));
        types.push_back(schema[i]->getDataType());
    }

    uint32_t prefix_sum = 0;
    for (uint32_t i = 0; i < offsets.size(); ++i) {
        uint32_t val = offsets[i];
        offsets[i] = prefix_sum;
        prefix_sum += val;
        IOTDB_DEBUG(std::string("Prefix Sum: ") + schema[i]->toString() + std::string(": ") +
                    std::to_string(offsets[i]));
    }

    str << "+----------------------------------------------------+" << std::endl;
    str << "|";
    for (uint32_t i = 0; i < schema.getSize(); ++i) {
        str << schema[i]->toString() << "|";
    }
    str << std::endl;
    str << "+----------------------------------------------------+" << std::endl;

    char* buf = (char*)buffer->buffer;
    for (uint32_t i = 0; i < buffer->num_tuples * buffer->tuple_size_bytes; i += buffer->tuple_size_bytes) {
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

bool C_CodeGenerator::generateCode(const DataSourcePtr& source, const PipelineContextPtr& context, std::ostream& out)
{

    input_schema_ = source->getSchema();

    StructDeclaration struct_decl_tuple_buffer = getStructDeclarationTupleBuffer();
    StructDeclaration struct_decl_state = getStructDeclarationWindowState();
    StructDeclaration struct_decl_tuple = getStructDeclarationInputTuple(input_schema_);

    context->addTypeDeclaration(struct_decl_tuple_buffer);
    context->addTypeDeclaration(struct_decl_state);
    context->addTypeDeclaration(struct_decl_tuple);

    //    code_.type_decls.push_back(struct_decl_tuple_buffer);
    //    code_.type_decls.push_back(struct_decl_state);
    //    code_.type_decls.push_back(struct_decl_tuple);

    code_.struct_decl_tuple_buffer = struct_decl_tuple_buffer;
    code_.struct_decl_state = struct_decl_state;
    code_.struct_decl_input_tuple = struct_decl_tuple;

    /* === declarations === */

    VariableDeclaration var_decl_tuple_buffers = VariableDeclaration::create(
        createPointerDataType(createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer))),
        "window_buffers");
    VariableDeclaration var_decl_tuple_buffer_output = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "output_tuple_buffer");
    VariableDeclaration var_decl_state =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_state)), "global_state");

    code_.var_decl_tuple_buffers = var_decl_tuple_buffers;
    code_.var_decl_tuple_buffer_output = var_decl_tuple_buffer_output;
    code_.var_decl_state = var_decl_state;

    /* Tuple *tuples; */

    VariableDeclaration var_decl_tuple =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_tuple)), "tuples");

    /* TupleBuffer *tuple_buffer_1; */
    VariableDeclaration var_decl_tuple_buffer_1 = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(getStructDeclarationTupleBuffer())), "tuple_buffer_1");

    /* uint64_t id = 0; */
    code_.var_decl_id = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(UINT64)), "id",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());
    /* int32_t ret = 0; */
    code_.var_decl_return = std::dynamic_pointer_cast<VariableDeclaration>(
        VariableDeclaration::create(createDataType(BasicType(INT32)), "ret",
                                    createBasicTypeValue(BasicType(INT32), "0"))
            .copy());

    code_.var_decl_input_tuple = var_decl_tuple;
    code_.variable_decls.push_back(var_decl_tuple);
    code_.variable_decls.push_back(var_decl_tuple_buffer_1);
    code_.variable_decls.push_back(*(code_.var_decl_return.get()));

    /* variable declarations for fields inside structs */
    // VariableDeclaration decl_field_campaign_id = struct_decl_tuple.getVariableDeclaration("campaign_id");
    VariableDeclaration decl_field_num_tuples_struct_tuple_buf =
        struct_decl_tuple_buffer.getVariableDeclaration("num_tuples");
    VariableDeclaration decl_field_data_ptr_struct_tuple_buf = struct_decl_tuple_buffer.getVariableDeclaration("data");

    code_.decl_field_num_tuples_struct_tuple_buf = decl_field_num_tuples_struct_tuple_buf;
    code_.decl_field_data_ptr_struct_tuple_buf = decl_field_data_ptr_struct_tuple_buf;

    /** init statements before for loop */

    /* tuple_buffer_1 = window_buffer[0]; */
    code_.variable_init_stmts.push_back(
        VarRefStatement(var_decl_tuple_buffer_1)
            .assign(VarRefStatement(var_decl_tuple_buffers)[ConstantExprStatement(INT32, "0")])
            .copy());
    /*  tuples = (Tuple *)tuple_buffer_1->data;*/
    code_.variable_init_stmts.push_back(
        VarRef(var_decl_tuple)
            .assign(TypeCast(
                VarRefStatement(var_decl_tuple_buffer_1).accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
                createPointerDataType(createUserDefinedType(struct_decl_tuple))))
            .copy());

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    code_.for_loop_stmt = std::make_shared<FOR>(
        *(code_.var_decl_id.get()),
        (VarRef(*(code_.var_decl_id.get())) <
         (VarRef(var_decl_tuple_buffer_1).accessPtr(VarRef(decl_field_num_tuples_struct_tuple_buf)))),
        ++VarRef(*(code_.var_decl_id.get())));
    code_.current_code_insertion_point = code_.for_loop_stmt->getCompoundStatement();

    code_.return_stmt = std::make_shared<ReturnStatement>(VarRefStatement(*code_.var_decl_return));

    return true;
}

bool C_CodeGenerator::generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out)
{
	
    ExpressionStatmentPtr expr = pred->generateCode(this->code_);

//    VariableDeclaration var_decl_i =
//        VariableDeclaration::create(createDataType(BasicType(INT32)), "my_int", createBasicTypeValue(BasicType(INT32), "1"));

//    bool found=false;
//    for(auto& decl : code_.variable_decls){
//        if(decl.getIdentifierName()==var_decl_i.getIdentifierName()){
//          found=true;
//        }
//    }
//    if(!found)
//      code_.variable_decls.push_back(var_decl_i);

    std::shared_ptr<IF> if_stmt = std::make_shared<IF>(*expr);
    CompoundStatementPtr compound_stmt = if_stmt->getCompoundStatement();
    /* update current compound_stmt*/
    code_.current_code_insertion_point->addStatement(if_stmt);
    code_.current_code_insertion_point=compound_stmt;

    return true;
}
bool C_CodeGenerator::generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out)
{

    result_schema_ = sink->getSchema();

    StructDeclaration struct_decl_result_tuple = getStructDeclarationResultTuple(result_schema_);
    context->addTypeDeclaration(struct_decl_result_tuple);
    code_.type_decls.push_back(struct_decl_result_tuple);
    VariableDeclaration var_decl_result_tuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_result_tuple)), "result_tuples");
    code_.variable_decls.push_back(var_decl_result_tuple);

    VariableDeclaration var_decl_num_result_tuples = VariableDeclaration::create(
        createDataType(BasicType(UINT64)), "num_result_tuples",createBasicTypeValue(BasicType(INT64), "0"));
    code_.variable_decls.push_back(var_decl_num_result_tuples);



    /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    code_.variable_init_stmts.push_back(
        VarRef(var_decl_result_tuple)
            .assign(TypeCast(VarRef(code_.var_decl_tuple_buffer_output)
                                 .accessPtr(VarRef(code_.decl_field_data_ptr_struct_tuple_buf)),
                             createPointerDataType(createUserDefinedType(struct_decl_result_tuple))))
            .copy());

    std::vector<VariableDeclaration> var_decls;
    std::vector<StatementPtr> write_result_tuples;
    for (size_t i = 0; i < result_schema_.getSize(); ++i) {
        VariableDeclarationPtr var_decl = getVariableDeclarationForField(struct_decl_result_tuple, result_schema_[i]);
        if (!var_decl) {
            IOTDB_FATAL_ERROR("Could not extract field " << result_schema_[i]->toString() << " from struct "
                                                         << struct_decl_result_tuple.getTypeName());
        }
        // VariableDeclaration var_decl =
        // code_.struct_decl_result_tuple.getVariableDeclaration(result_schema_[i]->name);
        code_.variable_decls.push_back(*var_decl);
        /** \FIXME: we need to handle the case where the field in the result tuple is not part of the input schema! */
        code_.current_code_insertion_point->addStatement(
            VarRef(var_decl_result_tuple)[VarRef(var_decl_num_result_tuples)]
                .accessRef(VarRef(*var_decl))
                .assign(VarRef(code_.var_decl_input_tuple)[VarRef(*(code_.var_decl_id))].accessRef(VarRef(*var_decl)))
                .copy());
        /* */
        code_.current_code_insertion_point->addStatement((++VarRef(var_decl_num_result_tuples)).copy());

        // var_decls.push_back(*var_decl);
        // write_result_tuples.push_back(VarRef(var_decl_result_tuple)[VarRef(*(code_.var_decl_id))].assign(VarRef(var_decl_result_tuple)[VarRef(*(code_.var_decl_id))]).copy());
    }

    /* TODO: set number of output tuples to result buffer */
    code_.cleanup_stmts.push_back(
    VarRef(code_.var_decl_tuple_buffer_output).accessPtr(VarRef(code_.decl_field_num_tuples_struct_tuple_buf)).assign(VarRef(var_decl_num_result_tuples)).copy()
        );
    return true;
}

PipelineStagePtr C_CodeGenerator::compile(const CompilerArgs&)
{

    /* function signature:
     * typedef uint32_t (*SharedCLibPipelineQueryPtr)(TupleBuffer**, WindowState*, TupleBuffer*);
     */

    // FunctionDeclaration main_function =
    FunctionBuilder func_builder = FunctionBuilder::create("compiled_query")
                                       .returns(createDataType(BasicType(UINT32)))
                                       .addParameter(code_.var_decl_tuple_buffers)
                                       .addParameter(code_.var_decl_state)
                                       .addParameter(code_.var_decl_tuple_buffer_output);

    for (auto& var_decl : code_.variable_decls) {
        func_builder.addVariableDeclaration(var_decl);
    }
    for (auto& var_init : code_.variable_init_stmts) {
        func_builder.addStatement(var_init);
    }

    /* here comes the code for the processing loop */
    func_builder.addStatement(code_.for_loop_stmt);

    /* add statements executed after the for loop, for example cleanup code */
    for (auto& stmt : code_.cleanup_stmts) {
        func_builder.addStatement(stmt);
    }

    /* add return statement */
    func_builder.addStatement(code_.return_stmt);

    FileBuilder file_builder = FileBuilder::create("query.cpp");
    /* add core declarations */
    file_builder.addDeclaration(code_.struct_decl_tuple_buffer);
    file_builder.addDeclaration(code_.struct_decl_state);
    file_builder.addDeclaration(code_.struct_decl_input_tuple);
    /* add generic declarations by operators*/
    for (auto& type_decl : code_.type_decls) {
        file_builder.addDeclaration(type_decl);
    }

    CodeFile file = file_builder.addDeclaration(func_builder.build()).build();

    PipelineStagePtr stage;
    stage = iotdb::compile(file);
    return stage;
}

C_CodeGenerator::~C_CodeGenerator() {}

CodeGeneratorPtr createCodeGenerator() { return std::make_shared<C_CodeGenerator>(CodeGenArgs()); }

} // namespace iotdb
