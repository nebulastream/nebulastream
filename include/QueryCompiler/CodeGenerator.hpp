#pragma once

#include "Operators/Operator.hpp"
#include <memory>

#include <API/Schema.hpp>
#include <API/Types/DataTypes.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <SourceSink/DataSink.hpp>

namespace NES {

class AttributeReference;
typedef std::shared_ptr<AttributeReference> AttributeReferencePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

// class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CompilerArgs;

class CompilerArgs {
};


class CodeGenerator {
  public:
    CodeGenerator();
    virtual bool generateCode(SchemaPtr schema, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const AttributeFieldPtr field,
                              const PredicatePtr& pred,
                              const NES::PipelineContextPtr& context,
                              std::ostream& out) = 0;
    virtual bool generateCodeForEmit(const SchemaPtr schema, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const WindowDefinitionPtr& window, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual ExecutablePipelinePtr compile(const CompilerArgs&, const GeneratedCodePtr& code) = 0;
    virtual ~CodeGenerator();

};

const StructDeclaration getStructDeclarationFromSchema(const std::string struct_name, SchemaPtr schema);
}// namespace NES
