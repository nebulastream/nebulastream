//
// Created by Adrian Michalke on 01.05.22.
//

#ifndef NES_QUERYMANANGER_ADHOCQUERY_HPP
#define NES_QUERYMANANGER_ADHOCQUERY_HPP

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include "../../../util/TestQuery.hpp"

/**
 * Run the ad hoc query
 *
 * @param uint64_t adhoc query number (1,...,7)
 * @param nodeEngine nodeEngine
 * @param testSchema schema of the source
 * @param queryId queryId to start the query with
 * @param mv MaterializedView to insert the tuples
 * @return uint64_t runtime in milliseconds
 */
template <typename MV>
static uint64_t runAdhocQuery(uint64_t adhocQueryNumber,
                                Runtime::NodeEnginePtr nodeEngine,
                                SchemaPtr testSchema,
                                uint64_t queryId,
                                std::shared_ptr<MV> mv) {
    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
            testSchema,
            [&](OperatorId id,
                    const SourceDescriptorPtr&,
                    const Runtime::NodeEnginePtr&,
                    size_t numSourceLocalBuffers,
                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
                return createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                     nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager(), id,
                                                                     numSourceLocalBuffers,
                                                                     std::move(successors));});

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(10, outputSchema, nodeEngine->getBufferManager());
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto sinkOperator = queryPlan->template getOperatorByType<LogicalUnaryOperatorNode>()[1];

    NES::QueryCompilation::PhysicalOperators::PhysicalOperatorPtr externalOperator;
    if (adhocQueryNumber == 1) {
        auto customPipelineStage = std::make_shared<AdHocQuery1<MV>>(mv);
        externalOperator =
                NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);
    } else {
        NES_ERROR("Ad hoc query number " << adhocQueryNumber << " is not supported");
        exit(1);
    }

    sinkOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    request->getQueryPlan()->setQueryId(queryId);
    request->getQueryPlan()->setQuerySubPlanId(queryId);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();

    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    plan->setup();

    auto start = std::chrono::high_resolution_clock::now();
    plan->start(nodeEngine->getStateManager());
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 64};
    plan->getPipelines()[0]->execute(buffer, workerContext);

    testSink->cleanupBuffers();
    buffer.release();
    plan->stop();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(); // return ms
}

#endif //NES_QUERYMANANGER_ADHOCQUERY_HPP