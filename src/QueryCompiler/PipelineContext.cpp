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

void PipelineContext::setWindow(Windowing::WindowHandlerPtr window) {
    this->windowHandler = std::move(window);
}

Windowing::WindowHandlerPtr PipelineContext::getWindow() {
    return this->windowHandler;
}

bool PipelineContext::hasWindow() const {
    return this->windowHandler != nullptr;
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