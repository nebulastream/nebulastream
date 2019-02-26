
#include <CodeGen/CodeGen.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>

#include <sstream>
#include <iostream>
#include <list>

//#include <clang/AST/AST.h>
//#include <clang/AST/Decl.h>
//#include <clang/Frontend/TextDiagnosticPrinter.h>
//#include <clang/Driver/Driver.h>
//#include <clang/Driver/DriverDiagnostic.h>

//#include <llvm/Support/Program.h>

//#include <clang/Basic/Builtins.h>
//#include <clang/Driver/Compilation.h>

#include <Util/ErrorHandling.hpp>

#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/UnaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>


namespace iotdb {


  class PipelineContext{
  public:
    void addTypeDeclaration(const Declaration&);
    void addVariableDeclaration(const Declaration&);
    std::vector<DeclarationPtr> type_declarations;
    std::vector<DeclarationPtr> variable_declarations;
  };

  void PipelineContext::addTypeDeclaration(const Declaration& decl){
    type_declarations.push_back(decl.copy());
  }
  void PipelineContext::addVariableDeclaration(const Declaration& decl){
    variable_declarations.push_back(decl.copy());
  }


  typedef std::shared_ptr<PipelineContext> PipelineContextPtr;



  class GeneratedCode {
   public:
    std::vector<VariableDeclaration> variable_decls;
    std::vector<StatementPtr> variable_init_stmts;
    std::shared_ptr<FOR> for_loop_stmt;
    std::vector<StatementPtr> cleanup_stmts;
    StatementPtr return_stmt;
    std::shared_ptr<VariableDeclaration> var_decl_id;
    std::shared_ptr<VariableDeclaration> var_decl_return;
  };

  typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

//  bool addToCode(GeneratedCodePtr code, GeneratedCodePtr code_to_add);

//  bool addToCode(GeneratedCodePtr code, GeneratedCodePtr code_to_add) {
//    if (!code || !code_to_add) return false;
//    code->header_and_types_code_block_
//        << code_to_add->header_and_types_code_block_.str();
//    code->fetch_input_code_block_ << code_to_add->fetch_input_code_block_.str();
//    code->declare_variables_code_block_
//        << code_to_add->declare_variables_code_block_.str();
//    code->init_variables_code_block_
//        << code_to_add->init_variables_code_block_.str();
//    code->upper_code_block_.insert(code->upper_code_block_.end(),
//                                   code_to_add->upper_code_block_.begin(),
//                                   code_to_add->upper_code_block_.end());
//    code->lower_code_block_.insert(code->lower_code_block_.begin(),
//                                   code_to_add->lower_code_block_.begin(),
//                                   code_to_add->lower_code_block_.end());
//    code->after_for_loop_code_block_
//        << code_to_add->after_for_loop_code_block_.str();
//    code->create_result_table_code_block_
//        << code_to_add->create_result_table_code_block_.str();
//    code->clean_up_code_block_ << code_to_add->clean_up_code_block_.str();
//    code->kernel_header_and_types_code_block_
//        << code_to_add->kernel_header_and_types_code_block_.str();
//    return true;
//  }

  CodeGenerator::CodeGenerator(const CodeGenArgs& args) : args_(args){

  }

  CodeGenerator::~CodeGenerator(){}

  class C_CodeGenerator : public CodeGenerator {
  public:
    C_CodeGenerator(const CodeGenArgs& args);
    virtual bool generateCode(const DataSourcePtr& source, const PipelineContextPtr& context, std::ostream& out) override;
    virtual bool generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) override;
    virtual bool generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) override;
    PipelineStagePtr compile(const CompilerArgs &) override;
    ~C_CodeGenerator() override;
  private:
    GeneratedCode code_;
  };

  C_CodeGenerator::C_CodeGenerator(const CodeGenArgs& args)
    : CodeGenerator (args)
  {
  }


  const StructDeclaration getStructDeclarationTupleBuffer(){
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

  const StructDeclaration getStructDeclarationWindowState(){
  /**
     define the WindowState struct
   struct WindowState {
    void *window_state;
    };
  */
  StructDeclaration struct_decl_state =
      StructDeclaration::create("WindowState", "")
          .addField(
              VariableDeclaration::create(createPointerDataType(createDataType(BasicType(VOID_TYPE))), "window_state"));
    return struct_decl_state;
  }

    const StructDeclaration getStructDeclarationInputTuple(){
  /* struct definition for input tuples */
  StructDeclaration struct_decl_tuple =
      StructDeclaration::create("Tuple", "")
          .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "campaign_id"));
    return struct_decl_tuple;
  }

  const StructDeclaration getStructDeclarationResultTuple(){
  /* struct definition for result tuples */
  StructDeclaration struct_decl_result_tuple =
      StructDeclaration::create("ResultTuple", "")
          .addField(VariableDeclaration::create(createDataType(BasicType(UINT64)), "sum"));
    return struct_decl_result_tuple;
  }


  bool C_CodeGenerator::generateCode(const DataSourcePtr& source, const PipelineContextPtr& context, std::ostream& out){

    input_schema_=source->getSchema();

    StructDeclaration struct_decl_tuple_buffer = getStructDeclarationTupleBuffer();
    StructDeclaration struct_decl_state = getStructDeclarationWindowState();
    StructDeclaration struct_decl_tuple = getStructDeclarationInputTuple();
    StructDeclaration struct_decl_result_tuple = getStructDeclarationResultTuple();

    context->addTypeDeclaration(struct_decl_tuple_buffer);
    context->addTypeDeclaration(struct_decl_state);
    context->addTypeDeclaration(struct_decl_tuple);
    context->addTypeDeclaration(struct_decl_result_tuple);

    /* === declarations === */

    VariableDeclaration var_decl_tuple_buffers = VariableDeclaration::create(
        createPointerDataType(createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer))), "window_buffer");
    VariableDeclaration var_decl_tuple_buffer_output = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_tuple_buffer)), "output_tuple_buffer");
    VariableDeclaration var_decl_state =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_state)), "global_state");

    /* Tuple *tuples; */

    VariableDeclaration var_decl_tuple =
        VariableDeclaration::create(createPointerDataType(createUserDefinedType(struct_decl_tuple)), "tuples");


    VariableDeclaration var_decl_result_tuple = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(struct_decl_result_tuple)), "result_tuples");

    /* TupleBuffer *tuple_buffer_1; */
    VariableDeclaration var_decl_tuple_buffer_1 = VariableDeclaration::create(
        createPointerDataType(createUserDefinedType(getStructDeclarationTupleBuffer())), "tuple_buffer_1");


    /* uint64_t id = 0; */
    code_.var_decl_id =
        std::dynamic_pointer_cast<VariableDeclaration>(VariableDeclaration::create(createDataType(BasicType(UINT64)), "id", createBasicTypeValue(BasicType(INT32), "0")).copy());
    /* int32_t ret = 0; */
    code_.var_decl_return =
        std::dynamic_pointer_cast<VariableDeclaration>(VariableDeclaration::create(createDataType(BasicType(INT32)), "ret", createBasicTypeValue(BasicType(INT32), "0")).copy());

    /* variable declarations for fields inside structs */
    VariableDeclaration decl_field_campaign_id = struct_decl_tuple.getVariableDeclaration("campaign_id");
    VariableDeclaration decl_field_num_tuples_struct_tuple_buf = struct_decl_tuple_buffer.getVariableDeclaration("num_tuples");
    VariableDeclaration decl_field_data_ptr_struct_tuple_buf = struct_decl_tuple_buffer.getVariableDeclaration("data");
    VariableDeclaration var_decl_field_result_tuple_sum = struct_decl_result_tuple.getVariableDeclaration("sum");

    /* init statements before for loop */

    /* tuple_buffer_1 = window_buffer[0]; */
    code_.variable_init_stmts.push_back(VarRefStatement(var_decl_tuple_buffer_1).assign(
                                                    VarRefStatement(var_decl_tuple_buffers)[ConstantExprStatement(INT32, "0")]).copy());
    /*  tuples = (Tuple *)tuple_buffer_1->data;*/
    BinaryOperatorStatement init_tuple_ptr(
        VarRef(var_decl_tuple).assign(
        TypeCast(VarRefStatement(var_decl_tuple_buffer_1)
          .accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
          createPointerDataType(createUserDefinedType(struct_decl_tuple)))));

     /* result_tuples = (ResultTuple *)output_tuple_buffer->data;*/
    BinaryOperatorStatement init_result_tuple_ptr(
        VarRef(var_decl_result_tuple).assign(
        TypeCast(VarRef(var_decl_tuple_buffer_output).accessPtr(VarRef(decl_field_data_ptr_struct_tuple_buf)),
                              createPointerDataType(createUserDefinedType(struct_decl_result_tuple)))));

    /* for (uint64_t id = 0; id < tuple_buffer_1->num_tuples; ++id) */
    code_.for_loop_stmt=std::make_shared<FOR>(
        *(code_.var_decl_id.get()), (VarRef(*(code_.var_decl_id.get())) < (VarRef(var_decl_tuple_buffer_1).accessPtr(
                                                 VarRef(decl_field_num_tuples_struct_tuple_buf)))),
        ++VarRef(*(code_.var_decl_id.get())));

    code_.return_stmt=std::make_shared<ReturnStatement>(VarRefStatement(*code_.var_decl_return));

    return true;
  }

  bool C_CodeGenerator::generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out){

    return true;
  }
  bool C_CodeGenerator::generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out){

    return true;
  }

  PipelineStagePtr C_CodeGenerator::compile(const CompilerArgs &){
      CCodeCompiler compiler;
      CompiledCCodePtr code = compiler.compile("");

      //code->getFunctionPointer("");
      return PipelineStagePtr();
  }

  C_CodeGenerator::~C_CodeGenerator(){

  }


  CodeGeneratorPtr createCodeGenerator(){
    return std::make_shared<C_CodeGenerator>(CodeGenArgs());
  }

}
