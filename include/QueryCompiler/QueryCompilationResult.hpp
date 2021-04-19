#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {

class QueryCompilationResult {
  public:
    static QueryCompilationResultPtr create();
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_
