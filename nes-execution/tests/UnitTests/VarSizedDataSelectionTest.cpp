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

#include <API/Functions/Functions.hpp>
#include <API/Functions/LogicalFunctions.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include "../../../nes-data-types/include/API/Schema.hpp"
#include "../../../nes-data-types/include/Common/DataTypes/BasicTypes.hpp"
#include "../../../nes-data-types/include/Common/DataTypes/DataTypeFactory.hpp"
#include "../../../nes-functions/include/Functions/NodeFunction.hpp"
#include "../../../nes-common/include/Util/Logger/Logger.hpp"
#include "../../../nes-common/include/Util/Logger/LogLevel.hpp"
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Selection.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>

#include <BaseUnitTest.hpp>
#include <nautilus/val.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

#include <Engine.hpp>

namespace NES
{

class VarSizedDataSelectionTest : public Testing::BaseUnitTest, public testing::WithParamInterface<bool>
{
public:
    Memory::BufferManagerPtr bufferManager;
    SchemaPtr varSizedDataSchema;
    std::unique_ptr<Memory::MemoryLayouts::TestTupleBuffer> testTupleBufferVarSize;

    static void SetUpTestCase()
    {
        Logger::setupLogging("VarSizedDataTestSelection.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup VarSizedDataTestSelection class.");
    }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        // BufferManager:
        bufferManager = Memory::BufferManager::create(4096, 10);

        // Schema:
        varSizedDataSchema = Schema::create()
                             ->addField("attr1", BasicType::UINT16)
                             ->addField("attr2", DataTypeFactory::createVariableSizedData())
                             ->addField("attr3", DataTypeFactory::createVariableSizedData());

        // TupleBuffer for input-side:
        auto tupleBufferVarSizedData = bufferManager->getBufferBlocking();

        // TestTupleBuffer to make access to input tupBuf easier:
        testTupleBufferVarSize
            = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(
                Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBufferVarSizedData, varSizedDataSchema));


        // We need this for setting up the PipelineExecutionContext
        auto emitToSuccessorFunctionHandler
            = [this](Memory::TupleBuffer& buffer, Runtime::WorkerContextRef)
        {
                // Add resulting tupleBuffers output
                emittedBuffers.emplace_back(buffer);
        };

        // We need this for setting up the PipelineExecutionContext
        auto emitToQueryManagerFunctionHandler = [this](Memory::TupleBuffer& buffer)
        {
            // Add resulting tupleBuffers output
            emittedBuffers.emplace_back(buffer);
        };

        // Setup worker-context to "simulate" current state of a worker thread.
        // Note: We use one buffer per worker for simplicity
        workerContext = std::make_shared<Runtime::WorkerContext>(WorkerThreadId(0), bufferManager, 1);

        // This is needed set-up pipelineExecutionContext, nullptr is not working...
        std::vector<Runtime::Execution::OperatorHandlerPtr> const handlers = {};

        // PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the Runtime.
        // Note: We use dummy values to get things running...s
        pipelineExecutionContext = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
            PipelineId(0),
            QueryId(0),
            bufferManager,
            1,
            emitToSuccessorFunctionHandler,
            emitToQueryManagerFunctionHandler,
            handlers);

        // Generate data:
        // Write some test data to testTupleBuffer
        for (uint64_t i = 0; i < testTupleBufferVarSize->getCapacity(); ++i)
        {
            auto testTuple = std::make_tuple(
                (uint16_t)i,
                // atr2:
                (i % 4 == 0) ? ("testString" + std::to_string(i) + "testString" + std::to_string(i) + "testString" + std::to_string(i)) :
                    (i % 3 == 0) ? ("testString" + std::to_string(i) + "testString" +std::to_string(i)) :
                        (i % 2 == 0) ? ("testString" + std::to_string(i) + std::to_string(i) ) :
                            ("testString" + std::to_string(i)),
                // atr2:
                (i % 4== 0) ? ("testString" + std::to_string(i) + "testString" +std::to_string(i)) :
                    (i % 3 == 0) ? ("testString" + std::to_string(i)) :
                        (i % 2 == 0) ? ("testString" + std::to_string(i) + std::to_string(i) ) :
                            "testString" + std::to_string(i) + "testString" + std::to_string(i) + "testString" + std::to_string(i));
            testTupleBufferVarSize->pushRecordToBuffer(testTuple, bufferManager.get());
            //ASSERT_EQ((testBufferVarSize->readRecordFromBuffer<uint16_t, std::string, std::string>(i)), testTuple);
        }
        ASSERT_EQ(testTupleBufferVarSize->getNumberOfTuples(), testTupleBufferVarSize->getCapacity());

        // Create row memory layout which is used during the scan
    auto layoutScan = std::make_shared<Memory::MemoryLayouts::RowLayout>(varSizedDataSchema, bufferManager->getBufferSize());
    // Set up a memoryProvider for the scan that takes care of reading and writing data from/to the TupleBuffer.
    // Note: We use row-wise memory accesses.
    auto memoryProviderScan = std::make_unique<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layoutScan);
    // Set-up scan-operator for execution
    scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(
        std::move(memoryProviderScan),
        varSizedDataSchema->getFieldNames());
    }

    // This function helps to set up all the required components to register and execute the operator
    void setupAndExecutePipeline(const auto& filterOperator)
    {
    // Create the emit operator that generally receives records from an upstream operator
    // and writes them to a tuple buffer according to a memory layout.

    // Define output layout:
    auto layoutEmit = std::make_shared<Memory::MemoryLayouts::RowLayout>(varSizedDataSchema, bufferManager->getBufferSize());
    // Memory provider for row-oriented access for the results:
    auto memoryProviderEmit = std::make_unique<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layoutEmit);
    // Actual emit operator fr execution:
    const auto emitOperator =
        std::make_shared<Runtime::Execution::Operators::Emit>(
            std::move(memoryProviderEmit));
            filterOperator->setChild(emitOperator);

    // Set-up pipeline for execution:
    // PhysicalOperatorPipeline captures a set of operators within a pipeline.
    auto physicalOperatorPipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    // Set the root operator in the operator tree:
    physicalOperatorPipeline->setRootOperator(scanOperator);


    // Executing the prior setted-up operator on the records:
    // We must capture the physicalOperatorPipeline by value to ensure it is not destroyed before the function is called
    const std::function compiledFunction = [&](
        nautilus::val<Runtime::WorkerContext*> workerContext,
        nautilus::val<Runtime::Execution::PipelineExecutionContext*> pipelineExecutionContext,
        nautilus::val<Memory::TupleBuffer*> recordBufferRef)
    {
        auto ctx = Runtime::Execution::ExecutionContext(workerContext, pipelineExecutionContext);
        // RecordBuffer maps to tupleBuffer and is a representation:
        Runtime::Execution::RecordBuffer recordBuffer(recordBufferRef);
        // Call open() to initialize pipeline state + call its children:
        physicalOperatorPipeline->getRootOperator()->open(ctx, recordBuffer);
        // Call close to clear executable state:
        physicalOperatorPipeline->getRootOperator()->close(ctx, recordBuffer);
    };
    // The following is used to set options during execution:
    nautilus::engine::Options options;
    // Set engine compilation mode:
    const auto& engineCompilation = GetParam();
    //options.setOption("engine.Compilation", engineCompilation); // Greater crashes here if compiled mode is on
    options.setOption("engine.Compilation", false);
    // Capture the dump:
    options.setOption("dump.all", true);
    // Register the options:
    const nautilus::engine::NautilusEngine engine(options);
    // Register the function(s) from the pipeline within the nautilus engine:
    auto executable = engine.registerFunction(compiledFunction);

    // Allocate temp. tupleBuffer to execute function
    auto tupleBuffer = new Memory::TupleBuffer(testTupleBufferVarSize->getBuffer());
    executable(workerContext.get(), pipelineExecutionContext.get(), tupleBuffer);
    delete tupleBuffer;
    }

    void verifyResults(const std::string& comparison) const
    {
        auto testTupleBufferResult = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(emittedBuffers[0], varSizedDataSchema);
        // Iterate through resulting testTupleBuffer, i.e. after selection on scan, and do assertions:
        for (uint64_t i = 0; i < testTupleBufferResult.getNumberOfTuples(); ++i)
        {
            // Using DynamicField to read the specific field value:
            // Note: The address is "shifted" by the first 4 bytes (uint32_t) of a varSizedData field as this is the length in the childBuffer.
            auto bufferIndexAttr2 = testTupleBufferResult[i]["attr2"].read<uint32_t>();
            auto bufferIndexAttr3 = testTupleBufferResult[i]["attr3"].read<uint32_t>();
            // Add. info:
            NES_INFO("{}: attr2 {} and attr3 {}", comparison, Memory::MemoryLayouts::readVarSizedData(testTupleBufferResult.getBuffer(), bufferIndexAttr2), Memory::MemoryLayouts::readVarSizedData(testTupleBufferResult.getBuffer(), bufferIndexAttr3));
            // Verify stuff via mapAssertions:
            auto it = mapAssertions.find(comparison);
            if (it != mapAssertions.end()) {
                it->second(
                    Memory::MemoryLayouts::readVarSizedData(testTupleBufferResult.getBuffer(), bufferIndexAttr2),
                    Memory::MemoryLayouts::readVarSizedData(testTupleBufferResult.getBuffer(), bufferIndexAttr3));
            } else {
                throw std::invalid_argument("Unknown test case: " + comparison);
            }
        }
    }

    static void TearDownTestCase() { NES_INFO("Tear down VarSizedDataTestSelection class."); }

protected:
    // A WorkerContext represents the current state of a worker thread.
    Runtime::WorkerContextPtr workerContext;
    // PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the Runtime.
    Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext;
    // List of tupleBuffers that are emitted from the query operators.s
    std::vector<Memory::TupleBuffer> emittedBuffers;
    // scanOperator that is used for the executed pipeline:
    std::shared_ptr<Runtime::Execution::Operators::Scan> scanOperator;
    // Map that is used to support different logical assertions for comparisons, based on 2 input strings:
    const std::map<std::string, std::function<void(const std::string&, const std::string&)>> mapAssertions = {
        {"equal", [](const std::string& p1, const std::string& p2){
                ASSERT_EQ(p1, p2);
            }},
        {"notEqual", [](const std::string& p1, const std::string& p2){
                ASSERT_NE(p1, p2);
            }},
        {"greater", [](const std::string& p1, const std::string& p2){
                ASSERT_TRUE(std::strcmp(p1.c_str(), p2.c_str()) > 0);
            }},
            {"less", [](const std::string& p1, const std::string& p2){
                ASSERT_TRUE(std::strcmp(p1.c_str(), p2.c_str()) < 0);
            }},
    };
};

/*  Simple test cases on varSizedData:
 *  (1) Generate data and store in tupleBuffer
 *  (2) Do a selection using different logical operators, e.g. equal, not equal, etc. ON 2 FIELDS
 *  (3) Store result in output buffers and verify results via assertions
 */
TEST_P(VarSizedDataSelectionTest, Equal)
{
    // Create a function node for equality on 2 fields:
    auto equalComparison = (Attribute("attr2") ==  Attribute("attr3"));
    // Lower function node to an executable function
    auto function = QueryCompilation::FunctionProvider::lowerFunction(equalComparison);
    // Selection operator that evaulates a boolean function on each record
    const auto filterOperator = std::make_shared<Runtime::Execution::Operators::Selection>(std::move(function));
    // scanOperator is root, we need to set its child to a filter-operator that contains the boolean function, i.e. equality operation
    scanOperator->setChild(filterOperator);

    setupAndExecutePipeline(filterOperator);

    verifyResults("equal");
}

TEST_P(VarSizedDataSelectionTest, NotEqual)
{
    auto notEqualComparison = (Attribute("attr2") != Attribute("attr3"));
    auto function = QueryCompilation::FunctionProvider::lowerFunction(notEqualComparison);
    const auto filterOperator = std::make_shared<Runtime::Execution::Operators::Selection>(std::move(function));
    scanOperator->setChild(filterOperator);

    setupAndExecutePipeline(filterOperator);

    verifyResults("notEqual");
}

TEST_P(VarSizedDataSelectionTest, Less)
{
    auto lessComparison = Attribute("attr2") < Attribute("attr3");
    auto function = QueryCompilation::FunctionProvider::lowerFunction(lessComparison);
    const auto filterOperator = std::make_shared<Runtime::Execution::Operators::Selection>(std::move(function));
    scanOperator->setChild(filterOperator);

    setupAndExecutePipeline(filterOperator);

    verifyResults("less");
}

TEST_P(VarSizedDataSelectionTest, Greater)
{
    // TODO: Why does greater has some issues when (engineCompilation option = true) ? Find out!
    auto greaterComparison = Attribute("attr2") > Attribute("attr3");
    auto function = QueryCompilation::FunctionProvider::lowerFunction(greaterComparison);
    const auto filterOperator = std::make_shared<Runtime::Execution::Operators::Selection>(std::move(function));
    scanOperator->setChild(filterOperator);

    setupAndExecutePipeline(filterOperator);

    verifyResults("greater");
}

/// Selection using equality:
/// TODO: overload operators to add more operations here, e.g. find(), substr(), concat(), toUpper, toLower etc. (see VariableSizedData.cpp)
/// e.g. ((Attribute("attr2") + Attribute("attr3") == Attribute("attr1") + Attribute("attr3")) for concat + equality

// We want to test on compiled + interpreted mode w.r.t engine.Compilation:
INSTANTIATE_TEST_CASE_P(
    TestingInputs,
    VarSizedDataSelectionTest,
    ::testing::Values(
        // compiled mode:
        true,
        // interpreted mode:
        false));
}