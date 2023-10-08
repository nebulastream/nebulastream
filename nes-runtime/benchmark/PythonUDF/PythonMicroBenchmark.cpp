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
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Project.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
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
    MicroBenchmarkRunner(std::string compiler, std::string pythonCompiler)
        : compiler(compiler), pythonCompiler(pythonCompiler) {

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
    auto initPipelineOperator(SchemaPtr inputSchema, SchemaPtr outputSchema, auto bufferManager) {
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bufferManager->getBufferSize());

        auto mapOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema);
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        scanOperator->setChild(mapOperator);

        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bufferManager->getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
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
            dynamicBuffer[i][variableName].write((T) (i % 10_s64));
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
    auto initMapHandler(std::string function,
                        std::string functionName,
                        std::string pythonCompiler,
                        SchemaPtr inputSchema,
                        SchemaPtr outputSchema) {
        return std::make_shared<Operators::PythonUDFOperatorHandler>(function, functionName, pythonCompiler, inputSchema, outputSchema);
    }

    virtual ~MicroBenchmarkRunner() = default;
    virtual void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) = 0;

  protected:
    uint64_t iterations = 10;
    //TPCH_Scale_Factor targetScaleFactor = TPCH_Scale_Factor::F1;
    std::string compiler;
    std::string pythonCompiler;
    ExecutablePipelineProvider* provider;
    Nautilus::CompilationOptions options;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    //std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
};

class SimpleFilterQueryNumericalNES : public MicroBenchmarkRunner {
  public:
    SimpleFilterQueryNumericalNES(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("x", BasicType::INT64);
        // schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto valueLeft = std::make_shared<Expressions::ConstantInt64ValueExpression>(10);
        auto valueRight = std::make_shared<Expressions::ReadFieldExpression>("x");
        auto equalsExpression = std::make_shared<Expressions::GreaterThanExpression>(valueRight, valueLeft);
        auto selectionOperator = std::make_shared<Operators::Selection>(equalsExpression);
        scanOperator->setChild(selectionOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        selectionOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto buffer = initInputBuffer<int64_t>("x", bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto pipelineContext = MockedPipelineExecutionContext();

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

class SimpleMapQueryNES : public MicroBenchmarkRunner {
  public:
    SimpleMapQueryNES(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        schema->addField("x", BasicType::INT64);
        // schema->addField("f2", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto valueLeft = std::make_shared<Expressions::ConstantInt64ValueExpression>(10);
        auto valueRight = std::make_shared<Expressions::ReadFieldExpression>("x");
        auto addExpression = std::make_shared<Expressions::GreaterThanExpression>(valueRight, valueLeft);
        auto writeFieldExpression = std::make_shared<Expressions::WriteFieldExpression>("x", addExpression);
        auto mapOperator = std::make_shared<Operators::Map>(writeFieldExpression);
        scanOperator->setChild(mapOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        mapOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto buffer = initInputBuffer<int64_t>("x", bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto pipelineContext = MockedPipelineExecutionContext();

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

class SimpleProjectionQueryNES : public MicroBenchmarkRunner {
  public:
    SimpleProjectionQueryNES(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("x", BasicType::INT64)
                          ->addField("y", BasicType::INT64)
                          ->addField("z", BasicType::INT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("x", BasicType::INT32);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());


        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto valueLeft = std::make_shared<Expressions::ConstantInt64ValueExpression>(10);
        auto valueRight = std::make_shared<Expressions::ReadFieldExpression>("x");
        auto addExpression = std::make_shared<Expressions::AddExpression>(valueRight, valueLeft);
        std::vector<Record::RecordFieldIdentifier> inputFields{"x", "y", "z"};
        std::vector<Record::RecordFieldIdentifier> outputFields{"x"};
        auto projectOperator = std::make_shared<Operators::Project>(inputFields, outputFields);
        scanOperator->setChild(projectOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        projectOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        /*
        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (int64_t i = 0; i < 100; i++) {
            dynamicBuffer[i]["x"].write(i % 10_s64);
            dynamicBuffer[i]["y"].write(+1_s64);
            dynamicBuffer[i]["z"].write(+1_s64);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }*/

        auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto pipelineContext = MockedPipelineExecutionContext();

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

class SimpleFilterQueryNumericalUDF : public MicroBenchmarkRunner {
  public:
    SimpleFilterQueryNumericalUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        std::string function = "def filter_numerical(x):"
                               "\n\treturn x > 10\n";
        std::string functionName = "filter_numerical";
        auto variableName = "x";
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(variableName, BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto pipeline = initPipelineOperator(schema, schema, bm);
        // auto dummyBuffer = NES::Runtime::TupleBuffer();
        auto buffer = initInputBuffer<int64_t>(variableName, bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, pythonCompiler, schema, schema);
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

class SimpleMapQueryUDF : public MicroBenchmarkRunner {
  public:
    SimpleMapQueryUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        std::string function = "def simple_map(x):"
                               "\n\ty = x + 10"
                               "\n\treturn y\n";
        std::string functionName = "simple_map";
        auto variableName = "x";
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(variableName, BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto pipeline = initPipelineOperator(schema, schema, bm);
        // auto dummyBuffer = NES::Runtime::TupleBuffer(); // from the TPCHBenchmark
        auto buffer = initInputBuffer<int64_t>(variableName, bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, pythonCompiler, schema, schema);
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

class SimpleProjectionQueryUDF : public MicroBenchmarkRunner {
  public:
    SimpleProjectionQueryUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        std::string function = "def simple_projection(x, y, z):"
                               "\n\treturn x\n";
        std::string functionName = "simple_projection";
        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("x", BasicType::INT64)
                          ->addField("y", BasicType::INT64)
                          ->addField("z", BasicType::INT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("x", BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());

        compileTimeTimer.start();
        auto pipeline = initPipelineOperator(inputSchema, outputSchema, bm);
        auto buffer = NES::Runtime::TupleBuffer(); // dummy buffer copied from TPCHBenchmark
        //auto buffer = initInputBuffer<int64_t>(variableName, bm, memoryLayout);

        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, pythonCompiler, inputSchema, outputSchema);
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

class NumbaExampleUDF : public MicroBenchmarkRunner {
    // using a modified example of the 5 minutes guide to check whether numba is working
    // https://numba.pydata.org/numba-doc/latest/user/5minguide.html
  public:
    NumbaExampleUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer) override {
        std::string function = "def go_fast(x):"
                               "\n\ta = np.arange(100).reshape(10, 10)"
                               "\n\ttrace = 0.0"
                               "\n\tfor i in range(a.shape[0]):"
                               "\n\t\ttrace += np.tanh(a[i, i])"
                               "\n\treturn x + trace + a";
        std::string functionName = "go_fast";
        auto variableName = "x";
        auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField(variableName, BasicType::INT64);
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

        compileTimeTimer.start();
        auto pipeline = initPipelineOperator(schema, schema, bm);
        // auto dummyBuffer = NES::Runtime::TupleBuffer(); // from the TPCHBenchmark
        auto buffer = initInputBuffer<int64_t>(variableName, bm, memoryLayout);
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, pythonCompiler, schema, schema);
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
    std::vector<std::string> pythonCompilers = {"default", "numba"};
    for (const auto& c : compilers) {
        for (const auto& pythonCompiler : pythonCompilers) {
            NES::Runtime::Execution::SimpleFilterQueryNumericalUDF(c, pythonCompiler).run();
            // NES::Runtime::Execution::SimpleFilterQueryNumericalNES(c, pythonCompiler).run();
            // NES::Runtime::Execution::SimpleMapQueryUDF(c, pythonCompiler).run();
            // NES::Runtime::Execution::SimpleMapQueryNES(c, pythonCompiler).run();
            // TODO fix projection queries...
            // NES::Runtime::Execution::SimpleProjectionQueryUDF(c, pythonCompiler).run();
            // NES::Runtime::Execution::SimpleProjectionQueryNES(c, pythonCompiler).run();
        }
    }
}