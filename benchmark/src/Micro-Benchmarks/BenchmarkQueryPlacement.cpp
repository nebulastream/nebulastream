/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <benchmark/benchmark.h>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include <z3++.h>

namespace NES::Benchmarking {

class PlacementStrategyHelper {
  public:
    PlacementStrategyHelper(uint64_t numberOfSources, uint8_t levels, uint8_t batchSize) {
        setupTopology(numberOfSources, levels, batchSize);
        typeInferencePhase = NES::Optimizer::TypeInferencePhase::create(streamCatalog);
    }

    NES::Optimizer::BasePlacementStrategyPtr getPlacementStrategy(const NES::GlobalExecutionPlanPtr& globalExecutionPlan,
                                                                  const std::string& placementStrategy) {
        return NES::Optimizer::PlacementStrategyFactory::getStrategy(placementStrategy,
                                                                     globalExecutionPlan,
                                                                     topology,
                                                                     typeInferencePhase,
                                                                     streamCatalog);
    }

    std::vector<NES::QueryPlanPtr> getSharedPlans(const std::vector<std::string>& queries) {
        auto globalQueryPlan = NES::GlobalQueryPlan::create();
        auto topologySpecificQueryRewrite = NES::Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
        auto queryReWritePhase = NES::Optimizer::QueryRewritePhase::create(false);
        z3::ContextPtr context = std::make_shared<z3::context>();
        auto signatureInferencePhase = NES::Optimizer::SignatureInferencePhase::create(
            context,
            NES::Optimizer::QueryMergerRule::Z3SignatureBasedPartialQueryMergerRule);
        auto syntacticQueryValidation = NES::Optimizer::SyntacticQueryValidation::create();
        std::vector<NES::QueryPlanPtr> sharedPlans;
        for (const auto& query : queries) {
            auto queryPlan = syntacticQueryValidation->checkValidityAndGetQuery(query)->getQueryPlan();
            queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());
            queryPlan = typeInferencePhase->execute(queryPlan);
            queryReWritePhase->execute(queryPlan);
            signatureInferencePhase->execute(queryPlan);
            queryPlan = topologySpecificQueryRewrite->execute(queryPlan);
            queryPlan = typeInferencePhase->execute(queryPlan);
            globalQueryPlan->addQueryPlan(queryPlan);
            auto signatureBasedPartialQueryMergerRule = NES::Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
            signatureBasedPartialQueryMergerRule->apply(globalQueryPlan);
            auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
            auto sharedQueryPlan = updatedSharedQMToDeploy[0]->getQueryPlan();
            sharedPlans.push_back(sharedQueryPlan);
            updatedSharedQMToDeploy[0]->markAsDeployed();
        }
        return sharedPlans;
    }

  private:
    NES::StreamCatalogPtr streamCatalog;
    NES::TopologyPtr topology;
    NES::Optimizer::TypeInferencePhasePtr typeInferencePhase;
    void setupTopology(uint64_t numberOfSources, uint8_t levels, uint8_t batchSize) {
        typeInferencePhase = NES::Optimizer::TypeInferencePhase::create(streamCatalog);
        NES_ASSERT2_FMT(levels > 1, "Levels must be greater than 2");
        topology = NES::Topology::create();
        streamCatalog = std::make_shared<NES::StreamCatalog>();
        auto schema = NES::Schema::create()
                          ->addField("id", UINT64)
                          ->addField("value", UINT64)
                          ->addField("timestamp", UINT64)
                          ->addField("originId", UINT64);
        const std::string streamName = "car";
        streamCatalog->addLogicalStream(streamName, schema);

        TopologyNodePtr rootNode = TopologyNode::create(UtilityFunctions::getNextTopologyNodeId(), "localhost", 123, 124, 63535);
        topology->setAsRoot(rootNode);
        std::vector<TopologyNodePtr> currentLevel;
        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setLogicalStreamName("car");

        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        for (uint64_t i = 0; i < numberOfSources; i++) {
            const TopologyNodePtr& source =
                TopologyNode::create(UtilityFunctions::getNextTopologyNodeId(), "localhost", 123, 124, 63535);
            currentLevel.push_back(source);
            sourceConfig->setPhysicalStreamName("test" + std::to_string(i));
            PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
            StreamCatalogEntryPtr streamCatalogEntry = std::make_shared<StreamCatalogEntry>(conf, source);
            streamCatalog->addPhysicalStream("car", streamCatalogEntry);
        }
        levels = levels - 1;
        for (; levels > 1; levels--) {
            std::vector<TopologyNodePtr> previousLevel{currentLevel.begin(), currentLevel.end()};
            currentLevel.clear();
            for (size_t i = 0; i < previousLevel.size(); i += batchSize) {
                auto last = std::min(previousLevel.size(), i + batchSize);
                auto parent = TopologyNode::create(UtilityFunctions::getNextTopologyNodeId(), "localhost", 123, 124, 63535);
                auto start = previousLevel.begin() + i;
                auto end = previousLevel.begin() + last;
                while (start != end) {
                    topology->addNewPhysicalNodeAsChild(parent, *start);
                    start++;
                }
                currentLevel.push_back(parent);
            }
        }
        if (levels == 1) {
            for (const auto& currentLevelNode : currentLevel) {
                topology->addNewPhysicalNodeAsChild(rootNode, currentLevelNode);
            }
        }
    }
};

static std::pair<PlacementStrategyHelper, std::vector<QueryPlanPtr>>
cacheSetup(uint64_t sources, uint8_t levels, uint8_t batchSize, const std::vector<std::string>& queries) {
    static std::mutex lock;
    static std::map<std::string, std::pair<PlacementStrategyHelper, std::vector<QueryPlanPtr>>> cache;
    static std::vector<QueryPlanPtr> sharedPlans;
    std::unique_lock<std::mutex> l(lock);
    std::stringstream ss;
    ss << sources << "|" << levels << "|" << batchSize << "|";
    for (const auto& query : queries) {
        ss << query << "|";
    }
    std::string key = ss.str();
    if (!cache.contains(key)) {
        PlacementStrategyHelper placementStrategyHelper(sources, levels, batchSize);
        sharedPlans = placementStrategyHelper.getSharedPlans(queries);
        cache.insert(
            {key,
             std::pair<PlacementStrategyHelper, std::vector<QueryPlanPtr>>(placementStrategyHelper, std::move(sharedPlans))});
    }
    return cache.at(key);
}

void BM_Complete_Placement(benchmark::State& state,
                           uint64_t sources,
                           uint8_t levels,
                           uint8_t batchSize,
                           const std::string& placement,
                           const std::vector<std::string>& queries) {
    NES::setupLogging("BenchmarkQueryPlacement.log", NES::LOG_FATAL);
    auto cache = cacheSetup(sources, levels, batchSize, queries);
    PlacementStrategyHelper placementStrategyHelper = cache.first;
    auto sharedPlans = cache.second;
    auto globalExecutionPlan = NES::GlobalExecutionPlan::create();
    auto placementStrategy = placementStrategyHelper.getPlacementStrategy(globalExecutionPlan, placement);
    placementStrategy->updateGlobalExecutionPlan(sharedPlans[0]);
    for (auto _ : state) {
        for (size_t i = 1; i < sharedPlans.size(); i++) {
            globalExecutionPlan = NES::GlobalExecutionPlan::create();
            placementStrategy = placementStrategyHelper.getPlacementStrategy(globalExecutionPlan, placement);
            placementStrategy->updateGlobalExecutionPlan(sharedPlans[i]);
        }
    }
}

void BM_Partial_Placement(benchmark::State& state,
                          uint64_t sources,
                          uint8_t levels,
                          uint8_t batchSize,
                          const std::string& placement,
                          const std::vector<std::string>& queries) {
    NES::setupLogging("BenchmarkQueryPlacement.log", NES::LOG_FATAL);
    auto cache = cacheSetup(sources, levels, batchSize, queries);
    PlacementStrategyHelper placementStrategyHelper = cache.first;
    auto sharedPlans = cache.second;
    auto globalExecutionPlan = NES::GlobalExecutionPlan::create();
    auto placementStrategy = placementStrategyHelper.getPlacementStrategy(globalExecutionPlan, placement);
    placementStrategy->updateGlobalExecutionPlan(sharedPlans[0]);
    for (auto _ : state) {
        for (size_t i = 1; i < sharedPlans.size(); i++) {
            placementStrategy = placementStrategyHelper.getPlacementStrategy(globalExecutionPlan, placement);
            placementStrategy->updateGlobalExecutionPlan(sharedPlans[i]);
        }
    }
}

#define REPETITIONS 5
#define NUMBER_OF_SOURCES 512
#define NUMBER_OF_LEVELS 10
#define CHILDREN_PER_PARENT 10

const std::vector<std::string> partialMerging{
    R"(Query::from("car").filter(Attribute("value") > 5).sink(PrintSinkDescriptor::create());)",
    R"(Query::from("car").filter(Attribute("value") > 5).map(Attribute("queryId") = 2).sink(PrintSinkDescriptor::create());)",
    R"(Query::from("car").filter(Attribute("value") > 5).map(Attribute("queryId") = 3).sink(PrintSinkDescriptor::create());)",
    R"(Query::from("car").filter(Attribute("value") > 5).map(Attribute("queryId") = 2).map(Attribute("newValue") = 20).sink(PrintSinkDescriptor::create());)",
};

const std::vector<std::string> equalQueries{
    R"(Query::from("car").filter(Attribute("value") > 5).map(Attribute("queryId") = 2).map(Attribute("newValue") = 20).sink(PrintSinkDescriptor::create());)",
    R"(Query::from("car").filter(Attribute("value") > 5).map(Attribute("queryId") = 2).map(Attribute("newValue") = 20).sink(PrintSinkDescriptor::create());)",
    R"(Query::from("car").filter(Attribute("value") > 5).map(Attribute("queryId") = 2).map(Attribute("newValue") = 20).sink(PrintSinkDescriptor::create());)",
    R"(Query::from("car").filter(Attribute("value") > 5).map(Attribute("queryId") = 2).map(Attribute("newValue") = 20).sink(PrintSinkDescriptor::create());)",
};

// 10 Level topology max node in each level has maximum of 50 children, TopDown
BENCHMARK_CAPTURE(BM_Complete_Placement,
                  partial_queries_complete_top_down_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Complete_Placement,
                  partial_queries_complete_bottom_up_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  partial_queries_partial_top_down_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  partial_queries_partial_bottom_up_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Complete_Placement,
                  equal_queries_complete_bottom_up_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Complete_Placement,
                  equal_queries_complete_top_down_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  equal_queries_partial_bottom_up_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  equal_queries_partial_top_down_hierarchical,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Complete_Placement,
                  partial_queries_complete_top_down_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Complete_Placement,
                  partial_queries_complete_bottom_up_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  partial_queries_partial_top_down_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  partial_queries_partial_bottom_up_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  partialMerging)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Complete_Placement,
                  equal_queries_complete_bottom_up_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Complete_Placement,
                  equal_queries_complete_top_down_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  equal_queries_partial_bottom_up_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("BottomUp"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

BENCHMARK_CAPTURE(BM_Partial_Placement,
                  equal_queries_partial_top_down_flat,
                  NUMBER_OF_SOURCES,
                  NUMBER_OF_LEVELS,
                  CHILDREN_PER_PARENT,
                  std::string("TopDown"),
                  equalQueries)
    ->Repetitions(REPETITIONS)
    ->DisplayAggregatesOnly(true);

int main(int argc, char** argv) {
    NESLogger->removeAllAppenders();
    NES::setupLogging("BenchmarkQueryPlacement.log", NES::LOG_DEBUG);

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}

}// namespace NES::Benchmarking

BENCHMARK_MAIN();
