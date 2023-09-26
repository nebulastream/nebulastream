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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <memory>

namespace NES::Runtime::Execution {

class MicroBenchmarkRunner {
  public:
    MicroBenchmarkRunner(std::string compiler)
        : compiler(compiler) {

        NES::Logger::setupLogging("MicroBenchmarkRunner.log", NES::LogLevel::LOG_DEBUG);
        provider = ExecutablePipelineProviderRegistry::getPlugin(compiler).get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 1000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        options.setOptimize(true);
        options.setDumpToFile(false);
    }
    void run() {
        double sumCompilation = 0;
        double sumExecution = 0;
        for (uint64_t i = 0; i < iterations; i++) {
            Timer compileTimeTimer("Compilation");
            Timer executionTimeTimer("Execution");
            runQuery(compileTimeTimer, executionTimeTimer);
            sumCompilation += compileTimeTimer.getPrintTime();
            sumExecution += executionTimeTimer.getPrintTime();
            NES_INFO("Run {} compilation time {}, execution time {}",
                      i,
                      compileTimeTimer.getPrintTime(),
                      executionTimeTimer.getPrintTime());
        }

        NES_INFO("Final {} compilation time {}, execution time {} ",
                  compiler,
                  (sumCompilation / (double) iterations),
                  (sumExecution / (double) iterations));
    };

    /**
     * Initializes a pipeline with a Scan of the input tuples, a MapPythonUDF operator, and a emit of the processed tuples.
     * @param schema Schema of the input and output tuples.
     * @param memoryLayout memory layout
     * @return
     */
    auto initPipelineOperator(SchemaPtr schema, auto memoryLayout) {
        auto mapOperator = std::make_shared<Operators::MapPythonUDF>(0, schema, schema);
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        scanOperator->setChild(mapOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        mapOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);
        return pipeline;
    }

    /**
     * Initializes the input buffer for numeric values.
     * @tparam T type of the numeric values
     * @param variableName name of the variable in the schema
     * @param bufferManager buffer manager
     * @param memoryLayout memory layout
     * @return input buffer
     */
    template<typename T>
    auto initInputBuffer(std::string variableName, auto bufferManager, auto memoryLayout) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t i = 0; i < 10; i++) {
            dynamicBuffer[i][variableName].write((T) i);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }
        return buffer;
    }

    /**
     * Initializes the map handler for the given pipeline.
     * @param className python class name of the udf
     * @param methodName method name of the udf
     * @param inputProxyName input proxy class name
     * @param outputProxyName output proxy class name
     * @param schema schema of the input and output tuples
     * @param testDataPath path to the test data containing the udf jar
     * @return operator handler
     */
    auto initMapHandler(std::string function, std::string functionName, SchemaPtr schema) {
        return std::make_shared<Operators::PythonUDFOperatorHandler>(function, functionName, schema, schema);
    }

    virtual ~MicroBenchmarkRunner() = default;
    virtual void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) = 0;

  protected:
    uint64_t iterations = 10;
    //TPCH_Scale_Factor targetScaleFactor = TPCH_Scale_Factor::F1;
    std::string compiler;
    ExecutablePipelineProvider* provider;
    Nautilus::CompilationOptions options;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    //std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
};

class SimpleFilterQueryNumerical : public MicroBenchmarkRunner {
  public:
    SimpleFilterQueryNumerical(std::string compiler) : MicroBenchmarkRunner(compiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        std::string function = "def filter_numerical(x):"
                               "\n\tif x > 10:"
                               "\n\t\treturn x\n";
        std::string functionName = "filter_numerical";
        auto variableName = "x";
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(variableName, BasicType::INT32);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto pipeline = initPipelineOperator(schema, memoryLayout);
        // auto dummyBuffer = NES::Runtime::TupleBuffer();
        auto buffer = initInputBuffer<int32_t>(variableName, bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, schema);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        executablePipeline->setup(pipelineContext);

        compileTimeTimer.snapshot("setup");

        compileTimeTimer.pause();
        executionTimeTimer.start();
        executablePipeline->execute(buffer, pipelineContext, *wc);

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);
    }
};

class SimpleMapQuery : public MicroBenchmarkRunner {
  public:
    SimpleMapQuery(std::string compiler) : MicroBenchmarkRunner(compiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        std::string function = "def simple_map(x):"
                               "\n\ty = x + 10.0"
                               "\n\treturn y\n";
        std::string functionName = "simple_map";
        auto variableName = "x";
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(variableName, BasicType::FLOAT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto pipeline = initPipelineOperator(schema, memoryLayout);
        // auto dummyBuffer = NES::Runtime::TupleBuffer(); // from the TPCHBenchmark
        auto buffer = initInputBuffer<double>(variableName, bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, schema);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        executablePipeline->setup(pipelineContext);

        compileTimeTimer.snapshot("setup");

        compileTimeTimer.pause();
        executionTimeTimer.start();
        executablePipeline->execute(buffer, pipelineContext, *wc);

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);
    }
};

class SimpleProjectionQuery : public MicroBenchmarkRunner {
  public:
    SimpleProjectionQuery(std::string compiler) : MicroBenchmarkRunner(compiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        std::string function = "def simple_projection(x, y, z):"
                               "\n\treturn x\n";
        std::string functionName = "simple_projection";
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("x", BasicType::INT32)
                          ->addField("y", BasicType::INT32)
                          ->addField("z", BasicType::INT32);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto pipeline = initPipelineOperator(schema, memoryLayout);
        auto buffer = NES::Runtime::TupleBuffer(); // dummy buffer copied from TPCHBenchmark
        // auto buffer = initInputBuffer<int_32t>(variableName, bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, schema);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        executablePipeline->setup(pipelineContext);

        compileTimeTimer.snapshot("setup");

        compileTimeTimer.pause();
        executionTimeTimer.start();
        executablePipeline->execute(buffer, pipelineContext, *wc);

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);
    }
};

}// namespace NES::Runtime::Execution

int main(int, char**) {
    std::vector<std::string> compilers = {"PipelineCompiler","CPPPipelineCompiler"};
    for (const auto& c : compilers) {
        NES::Runtime::Execution::SimpleFilterQueryNumerical(c).run();
    }
}