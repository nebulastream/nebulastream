#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHASEFACTORY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHASEFACTORY_HPP_
#include <QueryCompiler/Phases/PhaseFactory.hpp>

namespace NES {
namespace QueryCompilation {
namespace Phases {

class DefaultPhaseFactory : public PhaseFactory {
  public:
    static PhaseFactoryPtr create();
    const  TranslateToPhysicalOperatorsPtr createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) override;
    const PipeliningPhasePtr createPipeliningPhase(QueryCompilerOptionsPtr options) override;
    const AddScanAndEmitPhasePtr createAddScanAndEmitPhase(QueryCompilerOptionsPtr options) override;
    const TranslateToGeneratableOperatorsPtr createLowerPipelinePlanPhase(QueryCompilerOptionsPtr options) override;
    const CodeGenerationPhasePtr createCodeGenerationPhase(QueryCompilerOptionsPtr options) override;
};

}// namespace Phases
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHASEFACTORY_HPP_
