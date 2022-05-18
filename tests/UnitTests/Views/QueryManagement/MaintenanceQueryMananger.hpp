//
// Created by Adrian Michalke on 01.05.22.
//

#ifndef NES_QUERYMANANGER_MAINTENANCEQUERY_HPP
#define NES_QUERYMANANGER_MAINTENANCEQUERY_HPP

#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>

/**
 * Generate data for AIM benchmark
 * @param memAreaSize data size in bytes
 */
static uint8_t *generateData(uint64_t memAreaSize, SchemaPtr schema){
    std::default_random_engine generator;
    std::uniform_real_distribution<float> distribution(0.0,500.0);

    auto* memArea = reinterpret_cast<uint8_t*>(malloc(memAreaSize));
    auto* records = reinterpret_cast<EventRecord*>(memArea);
    size_t recordSize = schema->getSchemaSizeInBytes();
    size_t numRecords = memAreaSize / recordSize;
    NES_INFO("Generating " << numRecords << " Records" );
    for (auto i = 0U; i < numRecords; ++i) {
        //auto duration = std::chrono::system_clock::now().time_since_epoch();
        //auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

        records[i].mv$id = rand() % 9999;
        records[i].mv$cost = distribution(generator);
        records[i].mv$duration = rand() % 500;
        records[i].mv$timestamp = i;
        records[i].mv$isLongDistance = rand();
    }
    return memArea;
}

SourceDescriptorPtr getSourceDescriptor(SchemaPtr schema,
                                        Runtime::NodeEnginePtr nodeEngine,
                                        uint64_t bufferSize,
                                        uint64_t ingestionRate,
                                        bool csvSource = false) {
    SourceDescriptorPtr sourceDescriptor;
    if (csvSource){
        constexpr auto numBuffers = 0; // inf tuples
        sourceDescriptor = CsvSourceDescriptor::create(schema,
                                                       "/Users/adrianmichalke/workspace/masterthesis/nebulastream/tests/test_data/MV_test.csv",
                                                       ",", bufferSize, numBuffers, ingestionRate, true);
    } else {
        constexpr auto numBuffers = 1000; // inf tuples
        constexpr auto areaBuffers = 1000;
        assert(bufferSize == nodeEngine->getBufferManager()->getBufferSize());
        auto memoryAreaSize = areaBuffers * bufferSize;
        std::shared_ptr<uint8_t> memoryArea(generateData(memoryAreaSize, schema));
        // In contrast to csv source we have a fixed buffer size from the buffer manager
        sourceDescriptor = MemorySourceDescriptor::create(schema,
                                                          memoryArea,
                                                          memoryAreaSize,
                                                          numBuffers,
                                                          ingestionRate,
                                                          NES::BenchmarkSource::GatheringMode::INGESTION_RATE_MODE);
    }
    return sourceDescriptor;
}

/**
 * Start the maintenance query
 * @param testSink
 * @param nodeEngine
 * @param csvSchema
 * @param queryId
 * @param mv
 * @return chronos start time point
 */
template <typename MV>
static const std::chrono::high_resolution_clock::time_point
startMaintenanceQuery(std::shared_ptr<TestSink> testSink,
                      SourceDescriptorPtr sourceDescriptor,
                      Runtime::NodeEnginePtr nodeEngine,
                      uint64_t queryId,
                      std::shared_ptr<MV> mv) {

    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(sourceDescriptor).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto sinkOperator = queryPlan->getOperatorByType<LogicalUnaryOperatorNode>()[1];

    auto customPipelineStage = std::make_shared<MaintenanceQuery<MV>>(mv);
    auto externalOperator =
            NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    sinkOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->getQueryPlan()->setQueryId(queryId);
    request->getQueryPlan()->setQuerySubPlanId(queryId);
    auto plan = TestUtils::createTestQueryCompiler()->compileQuery(request)->getExecutableQueryPlan();
    nodeEngine->registerQueryInNodeEngine(plan);

    auto start = std::chrono::high_resolution_clock::now();
    nodeEngine->startQuery(queryId);
    return start;
}

/**
 * Stop the maintenance query
 * @param testSink
 * @param nodeEngine
 * @param queryId
 */
static void stopMaintenanceQuery(std::shared_ptr<TestSink> testSink,
                          Runtime::NodeEnginePtr nodeEngine,
                          uint64_t queryId){
    NES_INFO("Stop maintenance query");
    nodeEngine->stopQuery(queryId);
    testSink->cleanupBuffers();
}


#endif //NES_QUERYMANANGER_MAINTENANCEQUERY_HPP
