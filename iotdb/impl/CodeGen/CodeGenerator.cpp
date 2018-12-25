
#include <CodeGen/CodeGen.hpp>
#include <CodeGen/C_CodeGen/CodeCompiler.hpp>

namespace iotdb {

  class C_CodeGenerator : public CodeGenerator {
    bool addOperator(OperatorPtr, const CodeGenArgs &);
    PipelineStagePtr compile(const CompilerArgs &);
    ~C_CodeGenerator();
  };

  bool C_CodeGenerator::addOperator(OperatorPtr, const CodeGenArgs &){

    return false;
  }
  PipelineStagePtr C_CodeGenerator::compile(const CompilerArgs &){
      CCodeCompiler compiler;
      CompiledCCodePtr code = compiler.compile("");
      //code->getFunctionPointer("");
      return PipelineStagePtr();
  }
  C_CodeGenerator::~C_CodeGenerator(){

  }


}
