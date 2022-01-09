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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/CodeGenerator/RecordHandler.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/Logger.hpp>
#include <memory>

namespace NES::QueryCompilation {
PipelineContext::PipelineContext(PipelineContextArity arity) : arity(arity), recordHandler(RecordHandler::create()) {
    this->code = std::make_shared<GeneratedCode>();
}

PipelineContext::~PipelineContext() { NES_DEBUG("~PipelineContext(" + pipelineName + ")"); }

void PipelineContext::addVariableDeclaration(const Declaration& decl) { variable_declarations.push_back(decl.copy()); }

BlockScopeStatementPtr PipelineContext::createSetupScope() { return setupScopes.emplace_back(BlockScopeStatement::create()); }
BlockScopeStatementPtr PipelineContext::createStartScope() { return startScopes.emplace_back(BlockScopeStatement::create()); }

bool PipelineContext::hasNextPipeline() const { return !this->nextPipelines.empty(); }

RecordHandlerPtr PipelineContext::getRecordHandler() { return this->recordHandler; }

const std::vector<PipelineContextPtr>& PipelineContext::getNextPipelineContexts() const { return this->nextPipelines; }

void PipelineContext::addNextPipeline(const PipelineContextPtr& nextPipeline) { this->nextPipelines.push_back(nextPipeline); }

SchemaPtr PipelineContext::getInputSchema() const { return inputSchema; }

SchemaPtr PipelineContext::getResultSchema() const { return resultSchema; }

PipelineContextPtr PipelineContext::create() { return std::make_shared<PipelineContext>(); }

std::vector<BlockScopeStatementPtr> PipelineContext::getSetupScopes() { return setupScopes; }

std::vector<BlockScopeStatementPtr> PipelineContext::getStartScopes() { return startScopes; }

int64_t PipelineContext::registerOperatorHandler(const Runtime::Execution::OperatorHandlerPtr& operatorHandler) {
    operatorHandlers.emplace_back(operatorHandler);
    return operatorHandlers.size() - 1;
}

uint64_t PipelineContext::getHandlerIndex(const Runtime::Execution::OperatorHandlerPtr& operatorHandler) {
    for (auto i{0ul}; i < operatorHandlers.size(); ++i) {
        if (operatorHandlers[i] == operatorHandler) {
            return i;
        }
    }
    NES_FATAL_ERROR("Handler is not registered");
    return 0;
}

bool PipelineContext::getTuplePassesFiltersIsDeclared() { return this->tuplePassesFiltersIsDeclared; }

void PipelineContext::setTrueTuplePassesFiltersIsDeclared() { this->tuplePassesFiltersIsDeclared = true; }

std::vector<Runtime::Execution::OperatorHandlerPtr> PipelineContext::getOperatorHandlers() { return this->operatorHandlers; }

void PipelineContext::addHeaders(std::unordered_set<std::string> headers) {
    if (!headers.empty()) {
        for (auto itr : headers) {
            requiredHeaders.insert(itr);
        }
    }
}

std::unordered_set<std::string> PipelineContext::getRequiredHeaders() {
        return requiredHeaders;
    }
}// namespace NES::QueryCompilation