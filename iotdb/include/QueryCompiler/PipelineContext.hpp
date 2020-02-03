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

class Schema;

class PipelineContext {
 public:
  PipelineContext();
  void addTypeDeclaration(const Declaration &);
  void addVariableDeclaration(const Declaration &);
  std::vector<DeclarationPtr> type_declarations;
  std::vector<DeclarationPtr> variable_declarations;

  const Schema &getInputSchema() const;
  const Schema &getResultSchema() const;

  Schema inputSchema;
  Schema resultSchema;
  GeneratedCodePtr code;

  PipelineContextPtr getNextPipeline() const;
  void setNextPipeline(PipelineContextPtr nextPipeline);
  bool hasNextPipeline() const;

 private:
  PipelineContextPtr nextPipeline;
};
}
#endif //NES_INCLUDE_QUERYCOMPILER_PIPELINECONTEXT_HPP_
