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

class WindowHandler;
typedef std::shared_ptr<WindowHandler> WindowHandlerPtr;

/**
 * @brief The Pipeline Context represents the context of one pipeline during code generation.
 * todo it requires a refactoring if we redesign the compiler.
 */
class PipelineContext {
  public:
    PipelineContext();
    static PipelineContextPtr create();
    void addVariableDeclaration(const Declaration&);
    std::vector<DeclarationPtr> variable_declarations;

    SchemaPtr getInputSchema() const;
    SchemaPtr getResultSchema() const;
    WindowHandlerPtr getWindow();
    void setWindow(WindowHandlerPtr window);
    bool hasWindow() const;

    SchemaPtr inputSchema;
    SchemaPtr resultSchema;
    GeneratedCodePtr code;

    const std::vector<PipelineContextPtr>& getNextPipelineContexts() const;
    void addNextPipeline(PipelineContextPtr nextPipeline);
    bool hasNextPipeline() const;

    std::string pipelineName;

  private:
    std::vector<PipelineContextPtr> nextPipelines;
    WindowHandlerPtr windowHandler;
};
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
