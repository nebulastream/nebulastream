/*
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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/CodeGenerator/RecordHandler.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::QueryCompilation {
RuntimePipelineContext::RuntimePipelineContext(PipelineContextArity arity) : arity(arity), recordHandler(RecordHandler::create()) {
    this->code = std::make_shared<GeneratedCode>();
}

void RuntimePipelineContext::addVariableDeclaration(const Declaration& decl) { variable_declarations.push_back(decl.copy()); }

BlockScopeStatementPtr RuntimePipelineContext::createSetupScope() { return setupScopes.emplace_back(BlockScopeStatement::create()); }
BlockScopeStatementPtr RuntimePipelineContext::createStartScope() { return startScopes.emplace_back(BlockScopeStatement::create()); }

bool RuntimePipelineContext::hasNextPipeline() const { return !this->nextPipelines.empty(); }

RecordHandlerPtr RuntimePipelineContext::getRecordHandler() { return this->recordHandler; }

const std::vector<PipelineContextPtr>& RuntimePipelineContext::getNextPipelineContexts() const { return this->nextPipelines; }

void RuntimePipelineContext::addNextPipeline(const PipelineContextPtr& nextPipeline) { this->nextPipelines.push_back(nextPipeline); }

SchemaPtr RuntimePipelineContext::getInputSchema() const { return inputSchema; }

SchemaPtr RuntimePipelineContext::getResultSchema() const { return resultSchema; }

PipelineContextPtr RuntimePipelineContext::create() { return std::make_shared<RuntimePipelineContext>(); }

std::vector<BlockScopeStatementPtr> RuntimePipelineContext::getSetupScopes() { return setupScopes; }

std::vector<BlockScopeStatementPtr> RuntimePipelineContext::getStartScopes() { return startScopes; }

int64_t RuntimePipelineContext::registerOperatorHandler(const Runtime::Execution::OperatorHandlerPtr& operatorHandler) {
    operatorHandlers.emplace_back(operatorHandler);
    return operatorHandlers.size() - 1;
}

uint64_t RuntimePipelineContext::getHandlerIndex(const Runtime::Execution::OperatorHandlerPtr& operatorHandler) {
    for (auto i{0ul}; i < operatorHandlers.size(); ++i) {
        if (operatorHandlers[i] == operatorHandler) {
            return i;
        }
    }
    NES_FATAL_ERROR("Handler is not registered");
    return 0;
}

bool RuntimePipelineContext::getTuplePassesFiltersIsDeclared() { return this->tuplePassesFiltersIsDeclared; }

void RuntimePipelineContext::setTrueTuplePassesFiltersIsDeclared() { this->tuplePassesFiltersIsDeclared = true; }

std::vector<Runtime::Execution::OperatorHandlerPtr> RuntimePipelineContext::getOperatorHandlers() { return this->operatorHandlers; }
}
// namespace NES::QueryCompilation