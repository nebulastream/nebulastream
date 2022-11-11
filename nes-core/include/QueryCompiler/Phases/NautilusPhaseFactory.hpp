//
// Created by pgrulich on 10.11.22.
//

#ifndef NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSPHASEFACTORY_HPP_
#define NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSPHASEFACTORY_HPP_
#include <QueryCompiler/Phases/PhaseFactory.hpp>

namespace NES::QueryCompilation::Phases {

/**
 * @brief The default phase factory creates a default set of phases.
 */
class NautilusPhaseFactory : public PhaseFactory {
  public:
    virtual ~NautilusPhaseFactory() = default;
    static QueryCompilation::Phases::PhaseFactoryPtr create();
    QueryCompilation::LowerLogicalToPhysicalOperatorsPtr

    createLowerLogicalQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr options) override;
    QueryCompilation::PipeliningPhasePtr createPipeliningPhase(QueryCompilation::QueryCompilerOptionsPtr options) override;

    QueryCompilation::AddScanAndEmitPhasePtr
    createAddScanAndEmitPhase(QueryCompilation::QueryCompilerOptionsPtr options) override;

    QueryCompilation::LowerPhysicalToGeneratableOperatorsPtr
    createLowerPhysicalToGeneratableOperatorsPhase(QueryCompilation::QueryCompilerOptionsPtr options) override;

    QueryCompilation::CodeGenerationPhasePtr createCodeGenerationPhase(QueryCompilation::QueryCompilerOptionsPtr options,
                                                                       Compiler::JITCompilerPtr jitCompiler) override;
    QueryCompilation::LowerToExecutableQueryPlanPhasePtr
    createLowerToExecutableQueryPlanPhase(QueryCompilation::QueryCompilerOptionsPtr options, bool sourceSharing) override;
    QueryCompilation::BufferOptimizationPhasePtr
    createBufferOptimizationPhase(QueryCompilation::QueryCompilerOptionsPtr options) override;
    QueryCompilation::PredicationOptimizationPhasePtr
    createPredicationOptimizationPhase(QueryCompilation::QueryCompilerOptionsPtr options) override;

};

}// namespace NES::QueryCompilation::Phases

#endif//NES_NES_RUNTIME_INCLUDE_QUERYCOMPILER_NAUTILUSPHASEFACTORY_HPP_
