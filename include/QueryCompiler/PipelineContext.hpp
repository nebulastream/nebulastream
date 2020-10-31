#ifndef NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
#include <API/Schema.hpp>

namespace NES::Windowing {

class AbstractWindowHandler;
typedef std::shared_ptr<AbstractWindowHandler> WindowHandlerPtr;

}// namespace NES::Windowing

namespace NES {

class PipelineContext;
typedef std::shared_ptr<PipelineContext> PipelineContextPtr;

class Declaration;
typedef std::shared_ptr<Declaration> DeclarationPtr;

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

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
    Windowing::WindowHandlerPtr getWindow();
    void setWindow(Windowing::WindowHandlerPtr window);
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
    Windowing::WindowHandlerPtr windowHandler;
};
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
