/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_INCLUDE_QUERY_COMPILER_QUERY_COMPILER_HPP_
#define NES_INCLUDE_QUERY_COMPILER_QUERY_COMPILER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation {

/**
 * @brief General interface for the query compiler interface.
 * Subclasses can provide their own implementation on how to to process a query compilation request.
 */
class QueryCompiler {
  public:
    /**
     * @brief Submits a new query compilation request for compilation.
     * @param request The compilation request.
     * @return QueryCompilationResultPtr result for the query compilation.
     */
    virtual QueryCompilationResultPtr compileQuery(QueryCompilationRequestPtr request) = 0;
    virtual ~QueryCompiler() = default;

  protected:
    explicit QueryCompiler(QueryCompilerOptionsPtr const& options) noexcept : queryCompilerOptions(options) {}

    QueryCompilerOptionsPtr const queryCompilerOptions;
};

}// namespace NES::QueryCompilation

#endif  // NES_INCLUDE_QUERY_COMPILER_QUERY_COMPILER_HPP_
