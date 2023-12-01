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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Project.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
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
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>

namespace NES::Runtime::Execution {

class MicroBenchmarkRunner {
  public:
    MicroBenchmarkRunner(std::string compiler, std::string pythonCompiler)
        : compiler(compiler), pythonCompiler(pythonCompiler) {

        NES::Logger::setupLogging("MicroBenchmarkRunner.log", NES::LogLevel::LOG_DEBUG);
        provider = ExecutablePipelineProviderRegistry::getPlugin(compiler).get();
        table_bm = std::make_shared<Runtime::BufferManager>(100 * 1024 * 1024, 2048);
        bm = std::make_shared<Runtime::BufferManager>(10 * 1024 * 1024, 1024);
        wc = std::make_shared<WorkerContext>(0, bm, 1000);
        options.setOptimize(true);
        options.setDumpToFile(false);
    }
    void run() {
        double sumCompilation = 0;
        double sumExecution = 0;
        double sumPythonCompilation = 0;
        double sumPythonExecution = 0;
        for (uint64_t i = 0; i < iterations; i++) {
            Timer compileTimeTimer("Compilation");
            Timer executionTimeTimer("Execution");

            Timer pythonUDFCompilationTimeTimer("PythonUDFCompilation");
            runQuery(compileTimeTimer, executionTimeTimer, pythonUDFCompilationTimeTimer, sumPythonExecution);
            sumCompilation += compileTimeTimer.getPrintTime();
            sumExecution += executionTimeTimer.getPrintTime();
            sumPythonCompilation += pythonUDFCompilationTimeTimer.getPrintTime();
            NES_INFO("Run {} compilation time {}, execution time {}",
                      i,
                      compileTimeTimer.getPrintTime(),
                      executionTimeTimer.getPrintTime());
        }
        std::cout << pythonCompiler << std::endl << std::flush;

        NES_INFO("Final {} compilation time {}, execution time {}, python Compilation time {} ",
                  compiler,
                  (sumCompilation / (double) iterations),
                  (sumExecution / (double) iterations),
                 (sumPythonCompilation / (double) iterations));

        double totalCompilationTime = (sumCompilation / (double) iterations);
        double totalExecutionTime = (sumExecution / (double) iterations);
        double totalLatencyTime = totalCompilationTime + totalExecutionTime;
        double totalPythonCompilationTime = (sumPythonCompilation / (double) iterations);

        auto executeFile = std::filesystem::current_path().string() + "/dump/measurements.csv";
        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << getUDFName() << ", " << compiler << ", " << pythonCompiler << ", " << bufferSize << ", " << numberOfBuffers << ", " << totalCompilationTime << ", " << totalExecutionTime << ", " << totalLatencyTime << ", " << totalPythonCompilationTime << "\n";
        outputFileExecute.close();
    };

    /**
     * Initializes a pipeline with a Scan of the input tuples, a MapPythonUDF operator, and a emit of the processed tuples.
     * @param schema Schema of the input and output tuples.
     * @param memoryLayout memory layout
     * @return
     */
    auto initPipelineOperator(SchemaPtr inputSchema, SchemaPtr outputSchema, auto bufferManager) {
        auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bufferManager->getBufferSize());

        auto mapOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
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
        for (uint64_t i = 0; i < bufferSize; i++) {
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
                        std::map<std::string, std::string> modulesToImport,
                        std::string pythonCompiler,
                        SchemaPtr inputSchema,
                        SchemaPtr outputSchema, Timer<>& pythonUDFCompilationTimeTimer) {
        return std::make_shared<Operators::PythonUDFOperatorHandler>(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
    }

    virtual ~MicroBenchmarkRunner() = default;
    virtual void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) = 0;
    virtual std::string getUDFName() = 0;
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
    uint64_t bufferSize = 1024;
    // NES vs UDF 1250.0 Buffer
    // NES vs UDF Projection 350 Buffer
    // black scholes 250.0 Buffer
    // concat string 95 Buffer
    // word count and avg word length 95 Buffer
    // boolean 1000.0 Buffer
    // linear reg 1250.0 Buffer
    // kmeans 625.0 Buffer

    uint64_t numberOfBuffers = 95;
    //std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
};

class SimpleFilterQueryNumericalNES : public MicroBenchmarkRunner {
  public:
    SimpleFilterQueryNumericalNES(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        pythonUDFCompilationTimeTimer.start();
        pythonUDFCompilationTimeTimer.pause();
        sumPythonExecution += 0;
        NES_INFO("sumPython {}", sumPythonExecution);

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
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);
    }

    std::string getUDFName() override {
        return "nes_filter";
    }
};

class SimpleMapQueryNES : public MicroBenchmarkRunner {
  public:
    SimpleMapQueryNES(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        pythonUDFCompilationTimeTimer.start();
        pythonUDFCompilationTimeTimer.pause();
        sumPythonExecution += 0;
        NES_INFO("sumPython {}", sumPythonExecution);

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
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();
        executablePipeline->stop(pipelineContext);
    }

    std::string getUDFName() override {
        return "nes_map";
    }
};

class SimpleProjectionQueryNES : public MicroBenchmarkRunner {
  public:
    SimpleProjectionQueryNES(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        pythonUDFCompilationTimeTimer.start();
        pythonUDFCompilationTimeTimer.pause();
        sumPythonExecution += 0;
        NES_INFO("sumPython {}", sumPythonExecution);

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("x", BasicType::INT64)
                          ->addField("y", BasicType::INT64)
                          ->addField("z", BasicType::INT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("x", BasicType::INT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());


        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        std::vector<Record::RecordFieldIdentifier> inputFields{"x", "y", "z"};
        std::vector<Record::RecordFieldIdentifier> outputFields{"x"};
        auto projectOperator = std::make_shared<Operators::Project>(inputFields, outputFields);
        scanOperator->setChild(projectOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        projectOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);


        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(inputMemoryLayout, buffer);
        for (uint64_t i = 0; i < bufferSize; i++) {
            dynamicBuffer[i]["x"].write((int64_t) i % 10_s64);
            dynamicBuffer[i]["y"].write((int64_t) +1_s64);
            dynamicBuffer[i]["z"].write((int64_t) +1_s64);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto pipelineContext = MockedPipelineExecutionContext();

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);
    }

    std::string getUDFName() override {
        return "nes_projection";
    }
};

class SimpleFilterQueryNumericalUDF : public MicroBenchmarkRunner {
  public:
    SimpleFilterQueryNumericalUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {

        std::map<std::string, std::string> modulesToImport;
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
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, schema, schema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);

        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "filter_udf";
    }
};

class SimpleMapQueryUDF : public MicroBenchmarkRunner {
  public:
    SimpleMapQueryUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
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
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, schema, schema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "map_udf";
    }
};

class SimpleProjectionQueryUDF : public MicroBenchmarkRunner {
  public:
    SimpleProjectionQueryUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        std::string function = "def simple_projection(x, y, z):"
                               "\n\treturn x\n";
        std::string functionName = "simple_projection";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("x", BasicType::INT64)
                               ->addField("y", BasicType::INT64)
                               ->addField("z", BasicType::INT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("x", BasicType::INT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());


        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto projectOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(projectOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        projectOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);


        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(inputMemoryLayout, buffer);
        for (uint64_t i = 0; i < bufferSize; i++) {
            dynamicBuffer[i]["x"].write((int64_t) i % 10_s64);
            dynamicBuffer[i]["y"].write((int64_t) +1_s64);
            dynamicBuffer[i]["z"].write((int64_t) +1_s64);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "projection_udf";
    }
};

class NumbaExampleUDF : public MicroBenchmarkRunner {
    // using a modified example of the 5 minutes guide to check whether numba is working
    // https://numba.pydata.org/numba-doc/latest/user/5minguide.html
  public:
    NumbaExampleUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        //modulesToImport.insert({"numpy", "np"});
        std::string function = "def go_fast(x):"
                               "\n\ttotal = 0.0"
                               "\n\tfor i in range(10):"
                               "\n\t\tfor j in range(1,10):"
                               "\n\t\t\ttotal += (i/j)"
                               "\n\treturn x"; // cannot return numpy array atm
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
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, schema, schema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "double_loop";
    }
};

class BlackScholesNumPyUDF : public MicroBenchmarkRunner {
  public:
    BlackScholesNumPyUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        modulesToImport.insert({"numpy", "np"});
        std::string function = "def black_scholes_np_udf(stockPrice, optionStrike, optionYears, Riskfree, Volatility):\n"
                               "\tS = stockPrice\n"
                               "\tX = optionStrike\n"
                               "\tT = optionYears\n"
                               "\tR = Riskfree\n"
                               "\tV = Volatility\n"
                               "\tsqrtT = np.sqrt(T)\n"
                               "\td1 = (np.log(S / X) + (R + 0.5 * V * V) * T) / (V * sqrtT)\n"
                               "\td2 = d1 - V * sqrtT\n"
                               "\n"
                               "\tA1 = 0.31938153\n"
                               "\tA2 = -0.356563782\n"
                               "\tA3 = 1.781477937\n"
                               "\tA4 = -1.821255978\n"
                               "\tA5 = 1.330274429\n"
                               "\tRSQRT2PI = 0.39894228040143267793994605993438\n"
                               "\n"
                               "\tK = 1.0 / (1.0 + 0.2316419 * np.abs(d1))\n"
                               "\tret_val = (RSQRT2PI * np.exp(-0.5 * d1 * d1) * (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5))))))\n"
                               "\tcndd1 = np.where(d1 > 0, 1.0 - ret_val, ret_val)\n"
                               "\n"
                               "\tK = 1.0 / (1.0 + 0.2316419 * np.abs(d2))\n"
                               "\tret_val = (RSQRT2PI * np.exp(-0.5 * d2 * d2) * (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5))))))\n"
                               "\tcndd2 = np.where(d2 > 0, 1.0 - ret_val, ret_val)\n"
                               "\n"
                               "\texpRT = np.exp(- R * T)\n"
                               "\n"
                               "\tcallResult = S * cndd1 - X * expRT * cndd2\n"
                               "\tputResult = X * expRT * (1.0 - cndd2) - S * (1.0 - cndd1)\n"
                               "\treturn np.add(callResult, putResult).item()"; // cannot return numpy array atm
        std::string functionName = "black_scholes_np_udf";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("stockPrice", BasicType::FLOAT64)
                               ->addField("optionStrike", BasicType::FLOAT64)
                               ->addField("optionYears", BasicType::FLOAT64)
                               ->addField("Riskfree", BasicType::FLOAT64)
                               ->addField("Volatility", BasicType::FLOAT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("result", BasicType::FLOAT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());


        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto projectOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(projectOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        projectOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(inputMemoryLayout, buffer);

        // set seed
        std::random_device rd;
        std::mt19937 gen(rd());
        // set range
        std::uniform_real_distribution<> stockPrice(5, 30);
        std::uniform_real_distribution<> optionStrike(1, 100);
        std::uniform_real_distribution<> optionYears(0.25, 10);
        std::uniform_real_distribution<> riskAndVolatility(0, 1);
        for (uint64_t i = 0; i < bufferSize; i++) {
            // between 5 and 30
            dynamicBuffer[i]["stockPrice"].write(stockPrice(gen));
            // between 1 and 100
            dynamicBuffer[i]["optionStrike"].write(optionStrike(gen));
            // between 0.25 and 10
            dynamicBuffer[i]["optionYears"].write(optionYears(gen));
            // between 0 and 1
            dynamicBuffer[i]["Riskfree"].write(riskAndVolatility(gen));
            // between 0 and 1
            dynamicBuffer[i]["Volatility"].write(riskAndVolatility(gen));
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "black_scholes_numpy";
    }
};

class BlackScholesUDF : public MicroBenchmarkRunner {
  public:
    BlackScholesUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        modulesToImport.insert({"math", ""});
        std::string function = "def black_scholes_udf(stockPrice, optionStrike, optionYears, Riskfree, Volatility):\n"
                               "\tS = stockPrice\n"
                               "\tX = optionStrike\n"
                               "\tT = optionYears\n"
                               "\tR = Riskfree\n"
                               "\tV = Volatility\n"
                               "\tsqrtT = math.sqrt(T)\n"
                               "\td1 = (math.log(S / X) + (R + 0.5 * V * V) * T) / (V * sqrtT)\n"
                               "\td2 = d1 - V * sqrtT\n"
                               "\n"
                               "\tA1 = 0.31938153\n"
                               "\tA2 = -0.356563782\n"
                               "\tA3 = 1.781477937\n"
                               "\tA4 = -1.821255978\n"
                               "\tA5 = 1.330274429\n"
                               "\tRSQRT2PI = 0.39894228040143267793994605993438\n"
                               "\n"
                               "\tK = 1.0 / (1.0 + 0.2316419 * abs(d1))\n"
                               "\tret_val = (RSQRT2PI * math.exp(-0.5 * d1 * d1) * (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5))))))\n"
                               "\n"
                               "\tif (d1 > 0):\n"
                               "\t\tcndd1 = 1.0 - ret_val\n"
                               "\telse:\n"
                               "\t\tcndd1 = ret_val\n"
                               "\n"
                               "\tK = 1.0 / (1.0 + 0.2316419 * abs(d2))\n"
                               "\tret_val = (RSQRT2PI * math.exp(-0.5 * d2 * d2) * (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5))))))\n"
                               "\tif d2 > 0:\n"
                               "\t\tcndd2 = 1.0 - ret_val\n"
                               "\telse:\n"
                               "\t\tcndd2 = ret_val\n"
                               "\n"
                               "\texpRT = math.exp(- R * T)\n"
                               "\n"
                               "\tcallResult = S * cndd1 - X * expRT * cndd2\n"
                               "\tputResult = X * expRT * (1.0 - cndd2) - S * (1.0 - cndd1)\n"
                               "\treturn callResult + putResult";
        std::string functionName = "black_scholes_udf";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("stockPrice", BasicType::FLOAT64)
                               ->addField("optionStrike", BasicType::FLOAT64)
                               ->addField("optionYears", BasicType::FLOAT64)
                               ->addField("Riskfree", BasicType::FLOAT64)
                               ->addField("Volatility", BasicType::FLOAT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("result", BasicType::FLOAT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());


        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto projectOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(projectOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        projectOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(inputMemoryLayout, buffer);

        // set seed
        std::random_device rd;
        std::mt19937 gen(rd());
        // set range
        std::uniform_real_distribution<> stockPrice(5, 30);
        std::uniform_real_distribution<> optionStrike(1, 100);
        std::uniform_real_distribution<> optionYears(0.25, 10);
        std::uniform_real_distribution<> riskAndVolatility(0, 1);
        for (uint64_t i = 0; i < bufferSize; i++) {
            // between 5 and 30
            dynamicBuffer[i]["stockPrice"].write(stockPrice(gen));
            // between 1 and 100
            dynamicBuffer[i]["optionStrike"].write(optionStrike(gen));
            // between 0.25 and 10
            dynamicBuffer[i]["optionYears"].write(optionYears(gen));
            // between 0 and 1
            dynamicBuffer[i]["Riskfree"].write(riskAndVolatility(gen));
            // between 0 and 1
            dynamicBuffer[i]["Volatility"].write(riskAndVolatility(gen));
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "black_scholes";
    }
};

// string micro benchmarks

class StringConcatUDF : public MicroBenchmarkRunner {
  public:
    StringConcatUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        std::string function = "def concat_string(input):\n"
                               "\treturn input +\"test\"";
        std::string functionName = "concat_string";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("input", BasicType::TEXT);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("result", BasicType::TEXT);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());


        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto udfOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        udfOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto bufferMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, table_bm->getBufferSize());
        auto buffer = table_bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(bufferMemoryLayout, buffer);


        for (uint64_t i = 0; i < bufferSize; i++) {// string with 100 chars
            std::string value = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut l";
            auto varLengthBuffer = table_bm->getBufferBlocking();
            *varLengthBuffer.getBuffer<uint32_t>() = value.size();
            std::strcpy(varLengthBuffer.getBuffer<char>() + sizeof(uint32_t), value.c_str());
            auto index = buffer.storeChildBuffer(varLengthBuffer);
            dynamicBuffer[i]["input"].write(index);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "string_concat";
    }
};

class StringWordCountUDF : public MicroBenchmarkRunner {
  public:
    StringWordCountUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        std::string function = "def wordcount(sentence):\n"
                               "\twords = sentence.split(\" \")\n"
                               "\tlength = len(words)\n"
                               "\treturn length";
        std::string functionName = "wordcount";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("input", BasicType::TEXT);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("result", BasicType::INT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());

        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto udfOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        udfOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto bufferMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, table_bm->getBufferSize());
        auto buffer = table_bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(bufferMemoryLayout, buffer);

        // words with 100 chars
        std::string longWords = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut l";

        for (uint64_t i = 0; i < bufferSize; i++) {
            auto varLengthBuffer = table_bm->getBufferBlocking();
            *varLengthBuffer.getBuffer<uint32_t>() = longWords.size();
            std::strcpy(varLengthBuffer.getBuffer<char>() + sizeof(uint32_t), longWords.c_str());
            auto index = buffer.storeChildBuffer(varLengthBuffer);
            dynamicBuffer[i]["input"].write(index);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);

        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "word_count";
    }
};

class StringAverageWordLengthUDF : public MicroBenchmarkRunner {
  public:
    StringAverageWordLengthUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        std::string function = "def avg_word_length(sentence):\n"
                               "\twords = sentence.split()\n"
                               "\treturn sum(len(word) for word in words) / len(words)";
        std::string functionName = "avg_word_length";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("input", BasicType::TEXT);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("result", BasicType::FLOAT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());

        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto udfOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        udfOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto bufferMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, table_bm->getBufferSize());
        auto buffer = table_bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(bufferMemoryLayout, buffer);

        // words with 100 chars
        std::string longWords = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut l";

        for (uint64_t i = 0; i < bufferSize; i++) {
            auto varLengthBuffer = table_bm->getBufferBlocking();
            *varLengthBuffer.getBuffer<uint32_t>() = longWords.size();
            std::strcpy(varLengthBuffer.getBuffer<char>() + sizeof(uint32_t), longWords.c_str());
            auto index = buffer.storeChildBuffer(varLengthBuffer);
            dynamicBuffer[i]["input"].write(index);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "avg_word_length";
    }
};

class BooleanUDF : public MicroBenchmarkRunner {
  public:
    BooleanUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        std::string function = "def boolean_udf(check, value):\n"
                               "\tif check:\n"
                               "\t\treturn value\n"
                               "\telse:\n"
                               "\t\treturn 0";
        std::string functionName = "boolean_udf";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("check", BasicType::BOOLEAN)
                               ->addField("value", BasicType::INT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("result", BasicType::INT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());

        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto udfOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        udfOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto buffer = bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(inputMemoryLayout, buffer);

        for (uint64_t i = 0; i < bufferSize; i++) {
            if (i%3 == 0) {
                dynamicBuffer[i]["check"].write((bool) true);
            } else {
                dynamicBuffer[i]["check"].write((bool) false);
            }
            dynamicBuffer[i]["value"].write(i % 10_s64);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "boolean_udf";
    }
};

class LinearRegression : public MicroBenchmarkRunner {
  public:
    LinearRegression(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        modulesToImport.insert({"sklearn.datasets", ""});
        modulesToImport.insert({"sklearn.linear_model", ""});
        modulesToImport.insert({"numpy", "np"});

        std::string function = "def linear_regression_udf(x):\n"
                               "\tX = np.array([[1], [2], [3], [4]])\n"
                               "\ty =  np.array([14, 29, 10, 39])\n"
                               "\treg = sklearn.linear_model.LinearRegression().fit(X, y)\n"
                               "\treturn reg.predict([[x]]).item()";
        std::string functionName = "linear_regression_udf";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("x", BasicType::FLOAT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("prediction", BasicType::INT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());

        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto udfOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(udfOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        udfOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto bufferMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, table_bm->getBufferSize());
        auto buffer = initInputBuffer<double>("x", table_bm, bufferMemoryLayout);

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }

        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "linear_regression";
    }
};

class KMeansUDF : public MicroBenchmarkRunner {
  public:
    KMeansUDF(std::string compiler, std::string pythonCompiler) : MicroBenchmarkRunner(compiler, pythonCompiler){};

    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, Timer<>& pythonUDFCompilationTimeTimer, double sumPythonExecution) override {
        std::map<std::string, std::string> modulesToImport;
        modulesToImport.insert({"sklearn.cluster", ""});
        std::string function = "def kmeans_udf(x, y):\n"
                               "\tX = [[1, 2], [1, 4], [1, 0], [10, 2], [10, 4], [10, 0]]\n"
                               "\tkmeans = sklearn.cluster.KMeans(n_clusters=3, random_state=0, n_init=\"auto\").fit(X)\n"
                               "\tprediction = kmeans.predict([[x, y]]).item()\n"
                               "\treturn prediction";
        std::string functionName = "kmeans_udf";

        auto inputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                               ->addField("x", BasicType::INT64)
                               ->addField("y", BasicType::INT64);
        auto outputSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("prediction", BasicType::INT64);
        auto inputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, bm->getBufferSize());
        auto outputMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, bm->getBufferSize());


        compileTimeTimer.start();
        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(inputMemoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        auto projectOperator = std::make_shared<Operators::MapPythonUDF>(0, inputSchema, outputSchema, pythonCompiler);
        scanOperator->setChild(projectOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(outputMemoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        projectOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);

        auto buffer = table_bm->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(inputMemoryLayout, buffer);

        // set seed
        std::random_device rd;
        std::mt19937 gen(rd());
        // set range
        std::uniform_int_distribution<> xValue(0, 100);
        for (uint64_t i = 0; i < bufferSize; i++) {
            int64_t x = xValue(gen);
            int64_t y = xValue(gen);
            dynamicBuffer[i]["x"].write(x);
            dynamicBuffer[i]["y"].write(y);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }

        //auto buffer = NES::Runtime::TupleBuffer();
        auto executablePipeline = provider->create(pipeline, options);
        auto handler = initMapHandler(function, functionName, modulesToImport, pythonCompiler, inputSchema, outputSchema, pythonUDFCompilationTimeTimer);
        auto pipelineContext = MockedPipelineExecutionContext({handler});

        executablePipeline->setup(pipelineContext);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();

        executionTimeTimer.start();
        for (uint64_t i = 0; i < numberOfBuffers; i++) {
            executablePipeline->execute(buffer, pipelineContext, *wc);
        }
        executionTimeTimer.snapshot("execute");
        executionTimeTimer.pause();

        executablePipeline->stop(pipelineContext);


        sumPythonExecution += handler->getSumExecution();
        double avg_exec = (sumPythonExecution / (numberOfBuffers * bufferSize));
        NES_INFO("avg python udf execution time {}", avg_exec);
        auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";

        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << avg_exec << "\n";
        outputFileExecute.close();
    }

    std::string getUDFName() override {
        return "kmeans";
    }
};

}// namespace NES::Runtime::Execution

void deleteContentInFile(std::string filePath) {
    std::ofstream ofs1;
    ofs1.open(filePath, std::ofstream::out | std::ofstream::trunc);
    ofs1.close();
}

int main(int, char**) {
    std::vector<std::string> compilers = {"PipelineCompiler", "CPPPipelineCompiler"};
    std::vector<std::string> pythonCompilers = {"cpython", "cython", "nuitka", "numba", "pypy"};
    auto executeFile = std::filesystem::current_path().string() + "/dump/executepython.txt";
    deleteContentInFile(executeFile);

    // NES vs UDF 12500 Buffer
    // NES vs UDF Projection 4167 Buffer
    // black scholes 2500 Buffer
    // concat string 6250 Buffer
    // word count and avg word length 400 Buffer
    // boolean 10000 Buffer
    // linear reg 12500 Buffer
    // kmeans 6250 Buffer

    for (const auto& c : compilers) {
        for (const auto& pythonCompiler : pythonCompilers) {
            std::ofstream outputFileExecute;
            outputFileExecute.open(executeFile, std::ios_base::app);
            outputFileExecute << pythonCompiler << "-" << c << "\n";
            outputFileExecute.close();

            NES::Runtime::Execution::SimpleFilterQueryNumericalUDF(c, pythonCompiler).run();
            //NES::Runtime::Execution::SimpleFilterQueryNumericalNES(c, pythonCompiler).run();
            //NES::Runtime::Execution::SimpleMapQueryUDF(c, pythonCompiler).run();
            //NES::Runtime::Execution::SimpleMapQueryNES(c, pythonCompiler).run();

            //NES::Runtime::Execution::SimpleProjectionQueryUDF(c, pythonCompiler).run();
            //NES::Runtime::Execution::SimpleProjectionQueryNES(c, pythonCompiler).run();

            // nicht das
            //NES::Runtime::Execution::NumbaExampleUDF(c, pythonCompiler).run();

            //NES::Runtime::Execution::BlackScholesNumPyUDF(c, pythonCompiler).run();

            //NES::Runtime::Execution::BlackScholesUDF(c, pythonCompiler).run();

            //NES::Runtime::Execution::StringConcatUDF(c, pythonCompiler).run();

            //NES::Runtime::Execution::StringWordCountUDF(c, pythonCompiler).run();
            //NES::Runtime::Execution::StringAverageWordLengthUDF(c, pythonCompiler).run();

            //NES::Runtime::Execution::BooleanUDF(c, pythonCompiler).run();

            //NES::Runtime::Execution::LinearRegression(c, pythonCompiler).run();

            //NES::Runtime::Execution::KMeansUDF(c, pythonCompiler).run();
        }
    }
}
#endif