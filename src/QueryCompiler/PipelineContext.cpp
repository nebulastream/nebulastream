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

#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <memory>
#include <utility>

namespace NES {

PipelineContext::PipelineContext() { this->code = std::make_shared<GeneratedCode>(); }
void PipelineContext::addVariableDeclaration(const Declaration& decl) { variable_declarations.push_back(decl.copy()); }

BlockScopeStatementPtr PipelineContext::createSetupScope() { return setupScopes.emplace_back(BlockScopeStatement::create()); }
BlockScopeStatementPtr PipelineContext::createStartScope(){ return startScopes.emplace_back(BlockScopeStatement::create()); }

bool PipelineContext::hasNextPipeline() const { return this->nextPipelines.size() != 0; }

const std::vector<PipelineContextPtr>& PipelineContext::getNextPipelineContexts() const { return this->nextPipelines; }

void PipelineContext::addNextPipeline(PipelineContextPtr nextPipeline) { this->nextPipelines.push_back(nextPipeline); }

SchemaPtr PipelineContext::getInputSchema() const { return inputSchema; }

SchemaPtr PipelineContext::getResultSchema() const { return resultSchema; }

PipelineContextPtr PipelineContext::create() { return std::make_shared<PipelineContext>(); }

std::vector<BlockScopeStatementPtr> PipelineContext::getSetupScopes() { return setupScopes; }

std::vector<BlockScopeStatementPtr> PipelineContext::getStartScopes() {
    return startScopes;
}

int64_t PipelineContext::registerOperatorHandler(NodeEngine::Execution::OperatorHandlerPtr operatorHandler) {
    operatorHandlers.emplace_back(operatorHandler);
    return operatorHandlers.size() - 1;
}

const std::vector<NodeEngine::Execution::OperatorHandlerPtr> PipelineContext::getOperatorHandlers() {
    return this->operatorHandlers;
}

}// namespace NES