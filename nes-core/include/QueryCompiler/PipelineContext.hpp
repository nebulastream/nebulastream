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

#ifndef NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {

/**
 * @brief The Pipeline Context represents the context of one pipeline during code generation.
 * todo it requires a refactoring if we redesign the compiler.
 */
class PipelineContext {
  public:
    enum PipelineContextArity { Unary, BinaryLeft, BinaryRight };

    explicit PipelineContext(PipelineContextArity arity = Unary);
    ~PipelineContext();
    static PipelineContextPtr create();
    void addVariableDeclaration(const Declaration&);
    BlockScopeStatementPtr createSetupScope();
    BlockScopeStatementPtr createStartScope();
    std::vector<DeclarationPtr> variable_declarations;

    [[nodiscard]] SchemaPtr getInputSchema() const;
    [[nodiscard]] SchemaPtr getResultSchema() const;
    SchemaPtr inputSchema;
    SchemaPtr resultSchema;
    GeneratedCodePtr code;

    [[nodiscard]] const std::vector<PipelineContextPtr>& getNextPipelineContexts() const;
    void addNextPipeline(const PipelineContextPtr& nextPipeline);
    [[nodiscard]] bool hasNextPipeline() const;

    RecordHandlerPtr getRecordHandler();

    /**
     * @brief Appends a new operator handler and returns the handler index.
     * The index enables Runtime lookups.
     * @param operatorHandler
     * @return operator handler index
     */
    int64_t registerOperatorHandler(const Runtime::Execution::OperatorHandlerPtr& operatorHandler);

    std::vector<Runtime::Execution::OperatorHandlerPtr> getOperatorHandlers();

    uint64_t getHandlerIndex(const Runtime::Execution::OperatorHandlerPtr& operatorHandler);

    std::string pipelineName;
    PipelineContextArity arity;
    std::vector<BlockScopeStatementPtr> getSetupScopes();
    std::vector<BlockScopeStatementPtr> getStartScopes();

    /**
     * @brief Gets value of the variable tuplePassesPredicatesIsDeclared.
     */
    bool getTuplePassesFiltersIsDeclared();
    /**
     * @brief Sets the variable tuplePassesPredicatesIsDeclared to true, so that it does not declared again.
     */
    void setTrueTuplePassesFiltersIsDeclared();

  private:
    RecordHandlerPtr recordHandler;
    std::vector<PipelineContextPtr> nextPipelines;
    std::vector<BlockScopeStatementPtr> setupScopes;
    std::vector<BlockScopeStatementPtr> startScopes;
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
    bool tuplePassesFiltersIsDeclared = false;
};
}// namespace QueryCompilation
}// namespace NES
#endif  // NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
