/*
     Licensed under the Apache License, Version 2.0 (the "License");
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at

         https://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
     limitations under the License.
*/
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <ExportPhaseFactory.h>
#include <ExportQueryOptimizer.h>
#include <NoOpPhysicalSourceType.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <memory>

std::pair<NES::QueryPlanPtr, NES::GlobalExecutionPlanPtr>
ExportQueryOptimizer::optimize(NES::TopologyPtr topology,
                               NES::Configurations::CoordinatorConfigurationPtr config,
                               NES::QueryPlanPtr query,
                               NES::Catalogs::Source::SourceCatalogPtr sourceCatalog) {
    using namespace NES;
    using namespace NES::Runtime;

    Catalogs::UDF::UDFCatalogPtr udfCatalog = Catalogs::UDF::UDFCatalog::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto logicalSourceExpansionRule = NES::Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, true);
    auto queryRewritePhase = Optimizer::QueryRewritePhase::create(config);
    auto originInferencePhase = Optimizer::OriginIdInferencePhase::create();
    auto phaseFactory = QueryCompilation::Phases::ExportPhaseFactory::create();
    auto gep = GlobalExecutionPlan::create();

    query->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    auto queryPlan1 = queryRewritePhase->execute(query);
    auto queryPlan2 = originInferencePhase->execute(queryPlan1);
    auto queryPlan3 = logicalSourceExpansionRule->apply(queryPlan2);
    auto queryPlan = typeInferencePhase->execute(queryPlan3);

    auto sharedQueryPlan = NES::SharedQueryPlan::create(queryPlan);
    auto queryPlacementPhase = NES::Optimizer::QueryPlacementPhase::create(gep, topology, typeInferencePhase, config);

    queryPlacementPhase->execute(sharedQueryPlan);
    typeInferencePhase->execute(queryPlan);

    return {queryPlan, gep};
}
