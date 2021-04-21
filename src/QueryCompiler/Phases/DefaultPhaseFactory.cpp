
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/AlwaysBreakPolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseIfPossiblePolicy.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultGeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToGeneratbaleOperatorsPhase.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <QueryCompiler/Phases/CodeGenerationPhase.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace QueryCompilation {
namespace Phases {

PhaseFactoryPtr DefaultPhaseFactory::create() {
    return std::make_shared<DefaultPhaseFactory>();
}

const PipeliningPhasePtr DefaultPhaseFactory::createPipeliningPhase(QueryCompilerOptionsPtr options) {
    if (options->isOperatorFusionEnabled()) {
        NES_DEBUG("Create pipelining phase with fuse policy");
        auto operatorFusionPolicy = FuseIfPossiblePolicy::create();
        return DefaultPipeliningPhase::create(operatorFusionPolicy);
    } else {
        NES_DEBUG("Create pipelining phase with always break policy");
        auto operatorFusionPolicy = AlwaysBreakPolicy::create();
        return DefaultPipeliningPhase::create(operatorFusionPolicy);
    }
}

const TranslateToPhysicalOperatorsPtr DefaultPhaseFactory::createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create default lower logical plan phase");
    auto physicalOperatorProvider = DefaultPhysicalOperatorProvider::create();
    return TranslateToPhysicalOperators::create(physicalOperatorProvider);
}

const AddScanAndEmitPhasePtr DefaultPhaseFactory::createAddScanAndEmitPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create add scan and emit phase");
    return AddScanAndEmitPhase::create();
}
const TranslateToGeneratableOperatorsPtr DefaultPhaseFactory::createLowerPipelinePlanPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create default lower pipeline plan phase");
    auto generatableOperatorProvider = DefaultGeneratableOperatorProvider::create();
    return TranslateToGeneratableOperators::create(generatableOperatorProvider);
}
const CodeGenerationPhasePtr DefaultPhaseFactory::createCodeGenerationPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create default code generation phase");
    return CodeGenerationPhase::create();
}
const TranslateToExecutableQueryPlanPhasePtr
DefaultPhaseFactory::createLowerToExecutableQueryPlanPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create lower to executable query plan phase");
    return TranslateToExecutableQueryPlanPhase::create();
}
}// namespace Phases
}// namespace QueryCompilation
}// namespace NES