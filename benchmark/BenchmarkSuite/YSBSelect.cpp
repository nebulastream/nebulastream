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

#include "../../tests/util/DummySink.hpp"
#include "../../tests/util/TestQuery.hpp"
#include <filesystem>
#include <iostream>
#include <util/BenchmarkUtils.hpp>
#include <util/SimpleBenchmarkSink.hpp>
#include <util/YSBBenchmarkSource.hpp>
#include <vector>

#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Version/version.hpp>

using namespace NES;
using namespace NES::Benchmarking;

/**
 * @brief This file/main shows how a benchmark can be created. The benchmark seen below is a filter query that was implemented by using the BM_AddBenchmark macro from <util/BenchmarkUtils.hpp>.
 */
int main() {

    // All ingestion rates from 90M to 120M in a step range of 10M
    std::vector<uint64_t> allIngestionRates;//tuple per frequence period
                                            //    BenchmarkUtils::createRangeVector<uint64_t>(allIngestionRates, 1, 2, 1);
    //    BenchmarkUtils::createRangeVector<uint64_t>(allIngestionRates, 1000000, 100000000, 1000000);
    BenchmarkUtils::createRangeVector<uint64_t>(allIngestionRates, 25000000, 25000001, 10000000);

    std::vector<uint64_t> allExperimentsDuration;//all test
    BenchmarkUtils::createRangeVector<uint64_t>(allExperimentsDuration, 1, 2, 1);

    std::vector<uint64_t> allPeriodLengths;
    BenchmarkUtils::createRangeVector<uint64_t>(allPeriodLengths, 1, 2, 1);

    /**
     * allPeriodsLength => seconds, 1 => jede sekunde => set statically to 1
     *
     * allIngestionRates => tuple/pro Sekunde
     * allExperimentsDuration => seconds ein Lauf
     */

    std::vector<uint16_t> allWorkerThreads;
    BenchmarkUtils::createRangeVector<uint16_t>(allWorkerThreads, 2, 5, 1);

    std::vector<uint16_t> allDataSources;
    BenchmarkUtils::createRangeVector<uint16_t>(allDataSources, 2, 5, 1);

    std::string benchmarkFolderName = "YSBQuery" + BenchmarkUtils::getCurDateTimeStringWithNESVersion();
    if (!std::filesystem::create_directory(benchmarkFolderName)) {
        throw RuntimeException("Could not create folder " + benchmarkFolderName);
    }

    //-----------------------------------------Start of ----------------------------------------------------------------------------------------------
    std::vector<uint64_t> allSelectivities;
    BenchmarkUtils::createRangeVector<uint64_t>(allSelectivities, 0, 1, 1);

    auto benchmarkSchema = Schema::create()
                               ->addField("user_id", UINT64)
                               ->addField("page_id", UINT64)
                               ->addField("campaign_id", UINT64)
                               ->addField("ad_type", UINT64)
                               ->addField("event_type", UINT64)
                               ->addField("current_ms", UINT64)
                               ->addField("ip", UINT64)
                               ->addField("d1", UINT64)
                               ->addField("d2", UINT64)
                               ->addField("d3", UINT32)
                               ->addField("d4", UINT16);
#if 0
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, nullptr, 1);
    std::vector<DataSourcePtr> sources;
    for (int i = 0; i < 2; i++) {
        DataSourcePtr thisSource = (YSBBenchmarkSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), benchmarkSchema, 1, ++i));
        sources.push_back(thisSource);
    }
    auto query = TestQuery::from(benchmarkSchema).filter(Attribute("event_type") < 1).sink(DummySink::create());
    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    std::cout << " plan=" << queryPlan->toString() << std::endl;

    auto translatePhase = TranslateToGeneratableOperatorPhase::create();
    auto generateableOperators = translatePhase->transform(query.getQueryPlan()->getRootOperators()[0]);

    auto sink = SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager());
    auto builder = GeneratedQueryExecutionPlanBuilder::create()
        .setQueryManager(nodeEngine->getQueryManager())
        .setBufferManager(nodeEngine->getBufferManager())
        .setCompiler(nodeEngine->getCompiler())
        .setQueryId(1)
        .setQuerySubPlanId(1)
        .addSink(sink)
        .addOperatorQueryPlan(generateableOperators);

    for(auto src : sources)
    {
        builder.addSource(src);
    }

    auto plan = builder.build();
    nodeEngine->registerQueryInNodeEngine(plan);
    NES_INFO("QEP for " << queryPlan->toString() << " was registered in NodeEngine. Starting query now...");

    NES_INFO("Starting query...");
    nodeEngine->startQuery(1);
#endif

    //    for (auto selectivity : allSelectivities) {
    BM_AddBenchmark("BM_YSB_QUERY", TestQuery::from(thisSchema).filter(Attribute("event_type") < 1).sink(DummySink::create()),
                    YSBBenchmarkSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), benchmarkSchema,
                                               ingestionRate, 1),
                    SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()), ",X", ",ysb")
        //    }

        //-----------------------------------------End of BM_SimpleFilterQuery-----------------------------------------------------------------------------------------------

        return 0;
}