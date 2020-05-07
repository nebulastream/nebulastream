#ifndef NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
#include <API/Schema.hpp>
namespace NES {

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

class WindowDefinition;
typedef std::shared_ptr<WindowDefinition> WindowDefinitionPtr;

/**
 * @brief The Pipeline Context represents the context of one pipeline during code generation.
 * todo it requires a refactoring if we redesign the compiler.
 */
class PipelineContext {
  public:
    PipelineContext();
    void addVariableDeclaration(const Declaration&);
    std::vector<DeclarationPtr> variable_declarations;

    SchemaPtr getInputSchema() const;
    SchemaPtr getResultSchema() const;
    WindowDefinitionPtr getWindow();
    void setWindow(WindowDefinitionPtr window);
    bool hasWindow() const;

    SchemaPtr inputSchema;
    SchemaPtr resultSchema;
    GeneratedCodePtr code;

    PipelineContextPtr getNextPipeline() const;
    void setNextPipeline(PipelineContextPtr nextPipeline);
    bool hasNextPipeline() const;

  private:
    PipelineContextPtr nextPipeline;
    WindowDefinitionPtr windowDefinition;
};
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
