
#include <CodeGen/CodeGen.hpp>

namespace iotdb {




  class C_CodeGenerator : public CodeGenerator {
    bool addOperator(OperatorPtr, const CodeGenArgs &);
    PipelineStagePtr compile(const CompilerArgs &);
    ~C_CodeGenerator();
  };

}
