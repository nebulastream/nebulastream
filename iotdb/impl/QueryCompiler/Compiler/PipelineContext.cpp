#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <memory>
#include <utility>

namespace NES {

PipelineContext::PipelineContext() {
    this->code = std::make_shared<GeneratedCode>();
}
void PipelineContext::addTypeDeclaration(const Declaration& decl) { type_declarations.push_back(decl.copy()); }
void PipelineContext::addVariableDeclaration(const Declaration& decl) { variable_declarations.push_back(decl.copy()); }

void PipelineContext::setWindow(WindowDefinitionPtr window) {
    this->windowDefinition = std::move(window);
}

WindowDefinitionPtr PipelineContext::getWindow() {
    return this->windowDefinition;
}

bool PipelineContext::hasWindow() const {
    return this->windowDefinition != nullptr;
}

bool PipelineContext::hasNextPipeline() const {
    return this->nextPipeline != nullptr;
}

PipelineContextPtr PipelineContext::getNextPipeline() const {
    return this->nextPipeline;
}

void PipelineContext::setNextPipeline(PipelineContextPtr nextPipeline) {
    this->nextPipeline = nextPipeline;
}

SchemaPtr PipelineContext::getInputSchema() const { return inputSchema; }

SchemaPtr PipelineContext::getResultSchema() const { return resultSchema; }
}