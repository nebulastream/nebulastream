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
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <memory>
#include <utility>

namespace NES {

PipelineContext::PipelineContext() {
    this->code = std::make_shared<GeneratedCode>();
}
void PipelineContext::addVariableDeclaration(const Declaration& decl) { variable_declarations.push_back(decl.copy()); }

void PipelineContext::setWindow(Windowing::AbstractWindowHandlerPtr window) {
    this->windowHandler = std::move(window);
}

void PipelineContext::setJoin(Windowing::AbstractWindowHandlerPtr join) {
    this->joinHandler = std::move(join);
}

Windowing::AbstractWindowHandlerPtr PipelineContext::getWindow() {
    return this->windowHandler;
}

Windowing::AbstractWindowHandlerPtr PipelineContext::getJoin() {
    return this->joinHandler;
}


bool PipelineContext::hasWindow() const {
    return this->windowHandler != nullptr;
}

bool PipelineContext::hasJoin() const {
    return this->joinHandler != nullptr;
}
bool PipelineContext::hasNextPipeline() const {
    return this->nextPipelines.size() != 0;
}

const std::vector<PipelineContextPtr>& PipelineContext::getNextPipelineContexts() const {
    return this->nextPipelines;
}

void PipelineContext::addNextPipeline(PipelineContextPtr nextPipeline) {
    this->nextPipelines.push_back(nextPipeline);
}

SchemaPtr PipelineContext::getInputSchema() const { return inputSchema; }

SchemaPtr PipelineContext::getResultSchema() const { return resultSchema; }

PipelineContextPtr PipelineContext::create() { return std::make_shared<PipelineContext>(); }

}// namespace NES