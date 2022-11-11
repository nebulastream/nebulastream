#ifndef NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
#define NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/NautilusPhaseFactory.hpp>
namespace NES::Runtime::QueryCompiler {

class NautilusQueryCompiler : public QueryCompilation::QueryCompiler {
  public:
    QueryCompilation::QueryCompilationResultPtr compileQuery(QueryCompilation::QueryCompilationRequestPtr request) override;

  protected:
    NautilusQueryCompiler(QueryCompilation::QueryCompilerOptionsPtr const& options,
                          std::unique_ptr<Phases::NautilusPhaseFactory> const& phaseFactory,
                          bool sourceSharing);

    QueryCompilation::LowerLogicalToPhysicalOperatorsPtr lowerLogicalToPhysicalOperatorsPhase;
    QueryCompilation::LowerPhysicalToGeneratableOperatorsPtr lowerPhysicalToGeneratableOperatorsPhase;
    QueryCompilation::LowerToExecutableQueryPlanPhasePtr lowerToExecutableQueryPlanPhase;
    QueryCompilation::PipeliningPhasePtr pipeliningPhase;
    QueryCompilation::AddScanAndEmitPhasePtr addScanAndEmitPhase;
    bool sourceSharing;
};

}// namespace NES::Runtime::QueryCompiler

#endif//NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
