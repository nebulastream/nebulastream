
#include <NodeEngine/TupleBuffer.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

CodeGenerator::CodeGenerator() = default;

CodeGenerator::~CodeGenerator() = default;

CompilerTypesFactoryPtr CodeGenerator::getTypeFactory() {
    return std::make_shared<CompilerTypesFactory>();
}

}// namespace NES
