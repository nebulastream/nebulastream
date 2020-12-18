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
#include <API/Schema.hpp>

namespace NES::Windowing {

class AbstractWindowHandler;
typedef std::shared_ptr<AbstractWindowHandler> AbstractWindowHandlerPtr;

}// namespace NES::Windowing

namespace NES {

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

class BlockScopeStatement;
typedef std::shared_ptr<BlockScopeStatement> BlockScopeStatementPtr;

/**
 * @brief The Pipeline Context represents the context of one pipeline during code generation.
 * todo it requires a refactoring if we redesign the compiler.
 */
class PipelineContext {
  public:
    PipelineContext();
    ~PipelineContext(){
        NES_DEBUG("~PipelineContext("+pipelineName+")");
    }
    static PipelineContextPtr create();
    void addVariableDeclaration(const Declaration&);
    BlockScopeStatementPtr createSetupScope();
    BlockScopeStatementPtr createStartScope();
    std::vector<DeclarationPtr> variable_declarations;

    SchemaPtr getInputSchema() const;
    SchemaPtr getResultSchema() const;
    SchemaPtr inputSchema;
    SchemaPtr resultSchema;
    GeneratedCodePtr code;

    const std::vector<PipelineContextPtr>& getNextPipelineContexts() const;
    void addNextPipeline(PipelineContextPtr nextPipeline);
    bool hasNextPipeline() const;

    /**
     * @brief Appends a new operator handler and returns the handler index.
     * The index enables runtime lookups.
     * @param operatorHandler
     * @return operator handler index
     */
    int64_t registerOperatorHandler(NodeEngine::Execution::OperatorHandlerPtr operatorHandler);

    const std::vector<NodeEngine::Execution::OperatorHandlerPtr> getOperatorHandlers();

    std::string pipelineName;
    bool isLeftSide;
    std::vector<BlockScopeStatementPtr> getSetupScopes();
    std::vector<BlockScopeStatementPtr> getStartScopes();
  private:
    std::vector<PipelineContextPtr> nextPipelines;
    std::vector<BlockScopeStatementPtr> setupScopes;
    std::vector<BlockScopeStatementPtr> startScopes;
    std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers;
};
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
