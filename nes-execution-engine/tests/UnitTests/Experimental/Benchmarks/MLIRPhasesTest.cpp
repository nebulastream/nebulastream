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

#include "Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp"
#include "Util/Timer.hpp"
#include "Util/UtilityFunctions.hpp"
#include <API/Schema.hpp>
#include <Experimental/ExecutionEngine/CompilationBasedPipelineExecutionEngine.hpp>
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Expressions/ConstantIntegerExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
#include <Experimental/MLIR/MLIRUtility.hpp>
#include <Experimental/NESIR/Phases/LoopInferencePhase.hpp>
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Experimental/Trace/ExecutionTrace.hpp>
#include <Experimental/Trace/Phases/SSACreationPhase.hpp>
#include <Experimental/Trace/Phases/TraceToIRConversionPhase.hpp>
#include <Experimental/Trace/TraceContext.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <execinfo.h>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <Experimental/Utility/TestUtility.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

/**
 * @brief This test tests query execution using th mlir backend
 */
class MLIRPhasesTest : public testing::Test {
  public:
    Trace::SSACreationPhase ssaCreationPhase;
    Trace::TraceToIRConversionPhase irCreationPhase;
    IR::LoopInferencePhase loopInferencePhase;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MLIRPhasesTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup MLIRPhasesTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup MLIRPhasesTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down MLIRPhasesTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down MLIRPhasesTest test class." << std::endl; }
};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext()
        : PipelineExecutionContext(
            0,
            0,
            nullptr,
            [](Runtime::TupleBuffer&, Runtime::WorkerContextRef) {

            },
            [](Runtime::TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>()){};
};


void recursiveSnapshotIterationProcessing(std::shared_ptr<Timer<>> timer, 
                                          std::vector<std::vector<double>> &runningSnapshotVectors, 
                                          const int NUM_SNAPSHOTS) {
    auto snapshots = timer->getSnapshots();
    int numProcessedSnapshots = 0;
    int parentSnapshotIndex = 0;

    std::vector<std::vector<Timer<>::Snapshot>> snapshotQueue;
    snapshotQueue.emplace_back(timer->getSnapshots());
    while(numProcessedSnapshots < NUM_SNAPSHOTS-1) {
        bool skippedList = false;
        for(auto snapshot : snapshotQueue.back()) {
            // If current snapshot is nested, get children list, and remove snapshot.
            if(!snapshot.children.empty()) {
                //Todo Improve inefficient delete of current nested snapshot.
                snapshotQueue.back().erase(snapshotQueue.back().begin());
                snapshotQueue.emplace_back(snapshot.children);
                skippedList = true;
                break;
            } else {
                NES_DEBUG("Current Snapshot Name: " << snapshot.name << '\n');
                runningSnapshotVectors.at(numProcessedSnapshots).emplace_back(snapshot.getPrintTime());
                ++numProcessedSnapshots;
            }
        }
        // All elements of the current list were processed, we can remove it from the queue. 
        if(!skippedList) {
            snapshotQueue.pop_back();
        }
    }
    runningSnapshotVectors.at(NUM_SNAPSHOTS-1).emplace_back(timer->getPrintTime());
}

TEST_F(MLIRPhasesTest, phasedTPCHQ6) {
    
    auto testUtility = std::make_unique<NES::ExecutionEngine::Experimental::TestUtility>();
    auto bm = std::make_shared<Runtime::BufferManager>(100);
    auto lineitemBuffer = testUtility->loadLineItemTable(bm);

    auto runtimeWorkerContext = std::make_shared<Runtime::WorkerContext>(0, bm, 10);
    Scan scan = Scan(lineitemBuffer.first);
    /*
     *   l_shipdate >= date '1994-01-01'
     *   and l_shipdate < date '1995-01-01'
     */
    auto const_1994_01_01 = std::make_shared<ConstantIntegerExpression>(19940101);
    auto const_1995_01_01 = std::make_shared<ConstantIntegerExpression>(19950101);
    auto readShipdate = std::make_shared<ReadFieldExpression>(3);
    //Todo below is not correct? we are checking: 1994-01-01 < l_shipdate, but should be checking 1994-01-01 <= l_shipdate
    auto lessThanExpression1 = std::make_shared<LessThanExpression>(const_1994_01_01, readShipdate);
    auto lessThanExpression2 = std::make_shared<LessThanExpression>(readShipdate, const_1995_01_01);
    auto andExpression = std::make_shared<AndExpression>(lessThanExpression1, lessThanExpression2);

    // l_discount between 0.06 - 0.01 and 0.06 + 0.01
    auto readDiscount = std::make_shared<ReadFieldExpression>(2);
    auto const_0_05 = std::make_shared<ConstantIntegerExpression>(4);
    auto const_0_07 = std::make_shared<ConstantIntegerExpression>(8);
    auto lessThanExpression3 = std::make_shared<LessThanExpression>(const_0_05, readDiscount);
    auto lessThanExpression4 = std::make_shared<LessThanExpression>(readDiscount, const_0_07);
    auto andExpression2 = std::make_shared<AndExpression>(lessThanExpression3, lessThanExpression4);

    // l_quantity < 24
    auto const_24 = std::make_shared<ConstantIntegerExpression>(24);
    auto readQuantity = std::make_shared<ReadFieldExpression>(0);
    auto lessThanExpression5 = std::make_shared<LessThanExpression>(readQuantity, const_24);
    auto andExpression3 = std::make_shared<AndExpression>(andExpression, andExpression2);
    auto andExpression4 = std::make_shared<AndExpression>(andExpression3, lessThanExpression5);

    auto selection = std::make_shared<Selection>(andExpression4);
    scan.setChild(selection);

    // sum(l_extendedprice)
    auto aggField = std::make_shared<ReadFieldExpression>(1);
    auto sumAggFunction = std::make_shared<SumFunction>(aggField, IR::Types::StampFactory::createInt64Stamp());
    std::vector<std::shared_ptr<AggregationFunction>> functions = {sumAggFunction};
    auto aggregation = std::make_shared<Aggregation>(functions);
    selection->setChild(aggregation);

    //Setup timing, and results logging.
    const int NUM_ITERATIONS = 10;
    const int NUM_SNAPSHOTS = 9;
    const std::string RESULTS_FILE_NAME = "mlirPhases.csv";
    const std::vector<std::string> snapshotNames {
        "TraceGeneration       ",
        "NESIRGeneration       ",
        "MLIRGeneration        ",
        "MLIRLowering          ",
        "Lowering to LLVM IR   ",
        "JIT Compilation       ",
        "JIT Symbols Registered",
        "QueryExecutionTime    ",
        "Overall Time          "
    };

    std::vector<std::vector<double>> runningSnapshotVectors(NUM_SNAPSHOTS);

    // Execute workload NUM_ITERATIONS number of times.
    for(int i = 0; i < NUM_ITERATIONS; ++i) {
        std::shared_ptr<Timer<>> timer = std::make_shared<Timer<>>("TCP-H Q6 MLIR Phases Timer" + std::to_string(i));

        //Todo perform steps manually for easier acces to snapshots.
        auto executionEngine = CompilationBasedPipelineExecutionEngine();
        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(&scan);

        auto executablePipeline = executionEngine.compile(pipeline, timer);

        executablePipeline->setup();
        auto buffer = lineitemBuffer.second.getBuffer();
        timer->start();
        executablePipeline->execute(*runtimeWorkerContext, buffer);
        timer->snapshot("QueryExecutionTime");
        timer->pause();
        NES_INFO("QueryExecutionTime: " << timer);

        auto globalState = (GlobalAggregationState*) executablePipeline->getExecutionContext()->getGlobalOperatorState(0);
        auto sumState = (GlobalSumState*) globalState->threadLocalAggregationSlots[0].get();

        
        //Todo could add to testUtility, and also move 'runningSnapshotVectors' as member Var to TestUtility.
        int numProcessedSnapshots = 0;
        for(auto snapshot : timer->getSnapshots()) {
            runningSnapshotVectors.at(numProcessedSnapshots).emplace_back(snapshot.getPrintTime());
            ++numProcessedSnapshots;
        }
        runningSnapshotVectors.at(NUM_SNAPSHOTS-1).emplace_back(timer->getPrintTime());
        
        // ASSERT_EQ(sumState->sum, (int64_t) 204783021253); //Scale 1
        // ASSERT_EQ(sumState->sum, (int64_t) 1995906218); //Scale 0.01
    }
    testUtility->produceResults(runningSnapshotVectors, snapshotNames, RESULTS_FILE_NAME);
}


}// namespace NES::ExecutionEngine::Experimental::Interpreter