#ifndef NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
#define NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
#include <QueryCompiler/QueryCompiler.hpp>

namespace NES::QueryCompilation {
namespace Phases {
class NautilusPhaseFactory;
}

class LowerPhysicalToNautilusOperators;
using LowerPhysicalToNautilusOperatorsPtr = std::shared_ptr<LowerPhysicalToNautilusOperators>;
class NautilusCompilationPhase;
using NautilusCompilationPhasePtr = std::shared_ptr<NautilusCompilationPhase>;
class NautilusQueryCompiler : public QueryCompilation::QueryCompiler {
  public:
    QueryCompilation::QueryCompilationResultPtr compileQuery(QueryCompilation::QueryCompilationRequestPtr request) override;
    /**
     * @brief Creates a new instance of the DefaultQueryCompiler, with a set of options and phases.
     * @param options QueryCompilationOptions.
     * @param phaseFactory Factory which allows the injection of query optimization phases.
     * @param sourceSharing
     * @param useCompilationCache
     * @return QueryCompilerPtr
     */
    static QueryCompilerPtr
    create(QueryCompilerOptionsPtr const& options, Phases::PhaseFactoryPtr const& phaseFactory, bool sourceSharing = false);

  protected:
    NautilusQueryCompiler(QueryCompilation::QueryCompilerOptionsPtr const& options,
                          Phases::PhaseFactoryPtr const& phaseFactory,
                          bool sourceSharing);
    QueryCompilation::LowerLogicalToPhysicalOperatorsPtr lowerLogicalToPhysicalOperatorsPhase;
    QueryCompilation::LowerPhysicalToNautilusOperatorsPtr lowerPhysicalToNautilusOperatorsPhase;
    QueryCompilation::NautilusCompilationPhasePtr compileNautilusPlanPhase;
    QueryCompilation::LowerToExecutableQueryPlanPhasePtr lowerToExecutableQueryPlanPhase;
    QueryCompilation::PipeliningPhasePtr pipeliningPhase;
    QueryCompilation::AddScanAndEmitPhasePtr addScanAndEmitPhase;
    bool sourceSharing;
};

}// namespace NES::QueryCompilation

#endif//NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
