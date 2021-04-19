#ifndef NES_INCLUDE_QUERYCOMPILER_DEFAULTQUERYCOMPILER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_DEFAULTQUERYCOMPILER_HPP_
#include <QueryCompiler/NewQueryCompiler.hpp>
namespace NES {
namespace QueryCompilation {

class DefaultQueryCompiler : public QueryCompiler {
  public:
    static QueryCompilerPtr create(const QueryCompilerOptionsPtr options, const Phases::PhaseFactoryPtr phaseFactory);
    QueryCompilationResultPtr compileQuery(QueryCompilationRequestPtr request) override;
  protected:
    DefaultQueryCompiler(const QueryCompilerOptionsPtr options, const Phases::PhaseFactoryPtr phaseFactory);

    const TranslateToPhysicalOperatorsPtr translateToPhysicalOperatorsPhase;
    const TranslateToGeneratableOperatorsPtr translateToGeneratableOperatorsPhase;
    const PipeliningPhasePtr pipeliningPhase;
    const AddScanAndEmitPhasePtr addScanAndEmitPhase;
    const CodeGenerationPhasePtr codeGenerationPhase;
};
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_DEFAULTQUERYCOMPILER_HPP_
