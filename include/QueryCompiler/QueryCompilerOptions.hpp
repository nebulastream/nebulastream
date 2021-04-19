#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <cstdint>
namespace NES {
namespace QueryCompilation {

class QueryCompilerOptions {
  public:
    static QueryCompilerOptionsPtr createDefaultOptions();
    void enableOperatorFusion();
    void disableOperatorFusion();
    bool isOperatorFusionEnabled();
    void setNumSourceLocalBuffers(uint64_t num);
    uint64_t getNumSourceLocalBuffers();
  protected:
    bool operatorFusion;
    uint64_t numSourceLocalBuffers;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILEROPTIONS_HPP_
