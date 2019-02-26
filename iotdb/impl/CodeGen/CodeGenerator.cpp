
#include <CodeGen/CodeGen.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>

#include <sstream>
#include <iostream>
#include <list>

#include <clang/AST/AST.h>
#include <clang/AST/Decl.h>
#include <clang/Frontend/TextDiagnosticPrinter.h>
#include <clang/Driver/Driver.h>
#include <clang/Driver/DriverDiagnostic.h>

#include <llvm/Support/Program.h>

#include <clang/Basic/Builtins.h>
#include <clang/Driver/Compilation.h>

#include <Util/ErrorHandling.hpp>

namespace iotdb {

  class GeneratedCode {
   public:
    std::stringstream kernel_header_and_types_code_block_;
    std::stringstream header_and_types_code_block_;
    std::stringstream fetch_input_code_block_;
    std::stringstream declare_variables_code_block_;
    std::stringstream init_variables_code_block_;
    std::list<std::string> upper_code_block_;
    std::list<std::string> lower_code_block_;
    std::stringstream after_for_loop_code_block_;
    std::stringstream create_result_table_code_block_;
    std::stringstream clean_up_code_block_;
  };

  typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

  bool addToCode(GeneratedCodePtr code, GeneratedCodePtr code_to_add);

  bool addToCode(GeneratedCodePtr code, GeneratedCodePtr code_to_add) {
    if (!code || !code_to_add) return false;
    code->header_and_types_code_block_
        << code_to_add->header_and_types_code_block_.str();
    code->fetch_input_code_block_ << code_to_add->fetch_input_code_block_.str();
    code->declare_variables_code_block_
        << code_to_add->declare_variables_code_block_.str();
    code->init_variables_code_block_
        << code_to_add->init_variables_code_block_.str();
    code->upper_code_block_.insert(code->upper_code_block_.end(),
                                   code_to_add->upper_code_block_.begin(),
                                   code_to_add->upper_code_block_.end());
    code->lower_code_block_.insert(code->lower_code_block_.begin(),
                                   code_to_add->lower_code_block_.begin(),
                                   code_to_add->lower_code_block_.end());
    code->after_for_loop_code_block_
        << code_to_add->after_for_loop_code_block_.str();
    code->create_result_table_code_block_
        << code_to_add->create_result_table_code_block_.str();
    code->clean_up_code_block_ << code_to_add->clean_up_code_block_.str();
    code->kernel_header_and_types_code_block_
        << code_to_add->kernel_header_and_types_code_block_.str();
    return true;
  }

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
  };

  C_CodeGenerator::C_CodeGenerator(const CodeGenArgs& args)
    : CodeGenerator (args)
  {
  }

  bool C_CodeGenerator::generateCode(const DataSourcePtr& source, const PipelineContextPtr& context, std::ostream& out){

    input_schema_=source->getSchema();

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
