#ifndef NES_INCLUDE_QUERYCOMPILER_NEWQUERYCOMPILER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_NEWQUERYCOMPILER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {

/**
 * @brief Query compiler interface.
 */
class QueryCompiler {
  public:
    /**
     * @brief Submits a new query compilation request for compilation.
     * @param request The compilation request.
     * @return QueryCompilationResultPtr result for the query compilation.
     */
    virtual QueryCompilationResultPtr compileQuery(QueryCompilationRequestPtr request) = 0;
  protected:
    QueryCompiler(const QueryCompilerOptionsPtr);
    const QueryCompilerOptionsPtr options;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_NEWQUERYCOMPILER_HPP_
