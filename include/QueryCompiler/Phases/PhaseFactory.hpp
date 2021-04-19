#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
namespace Phases {

class PhaseFactory {
  public:
    virtual const TranslateToPhysicalOperatorsPtr createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const PipeliningPhasePtr createPipeliningPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const AddScanAndEmitPhasePtr createAddScanAndEmitPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const TranslateToGeneratableOperatorsPtr createLowerPipelinePlanPhase(QueryCompilerOptionsPtr options) = 0;
    virtual const CodeGenerationPhasePtr createCodeGenerationPhase(QueryCompilerOptionsPtr options) = 0;
};

}// namespace Phases
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHASES_PHASEFACTORY_HPP_
