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
#include "Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp"
#include <API/Schema.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TPCH/Query6PythonUDF.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class BenchmarkRunner {
  public:
    BenchmarkRunner(TPCH_Scale_Factor targetScaleFactor, std::string compiler,  std::string pythonCompiler)
        : targetScaleFactor(targetScaleFactor), compiler(compiler), pythonCompiler(pythonCompiler) {

        NES::Logger::setupLogging("BenchmarkRunner.log", NES::LogLevel::LOG_DEBUG);
        provider = ExecutablePipelineProviderRegistry::getPlugin(compiler).get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 2500);
        bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 2000);
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        tables = TPCHTableGenerator(table_bm, targetScaleFactor).generate();
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
            runQuery(compileTimeTimer, executionTimeTimer, &sumPythonCompilation, &sumPythonExecution);
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

        double totalCompilationTime = (sumCompilation / (double) iterations);
        double totalExecutionTime = (sumExecution / (double) iterations);
        double totalLatencyTime = totalCompilationTime + totalExecutionTime;
        double totalPythonCompilationTime = (sumPythonCompilation / (double) iterations);
        double totalPythonExecutionTime = (sumPythonExecution / (double) iterations);

        auto executeFile = std::filesystem::current_path().string() + "/dump/measurements_tpc_h.csv";
        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << "tpc_h_query6" << ", " << compiler << ", " << pythonCompiler << ", " << totalCompilationTime << ", " << totalExecutionTime << ", " << totalLatencyTime << ", " << totalPythonCompilationTime << ", " << totalPythonExecutionTime << "\n";
        outputFileExecute.close();
    };

    virtual ~BenchmarkRunner() = default;
    virtual void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, double* sumPythonCompilation, double* sumPythonExecution) = 0;

  protected:
    uint64_t iterations = 10;
    TPCH_Scale_Factor targetScaleFactor = TPCH_Scale_Factor::F1;
    std::string compiler;
    std::string pythonCompiler;
    ExecutablePipelineProvider* provider;
    Nautilus::CompilationOptions options;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
};

class BenchmarkRunnerMultipleTimer {
  public:
    BenchmarkRunnerMultipleTimer(TPCH_Scale_Factor targetScaleFactor, std::string compiler,  std::string pythonCompiler)
        : targetScaleFactor(targetScaleFactor), compiler(compiler), pythonCompiler(pythonCompiler) {

        NES::Logger::setupLogging("BenchmarkRunnerMultipleTimer.log", NES::LogLevel::LOG_DEBUG);
        provider = ExecutablePipelineProviderRegistry::getPlugin(compiler).get();
        table_bm = std::make_shared<Runtime::BufferManager>(8 * 1024 * 1024, 1000);
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        tables = TPCHTableGenerator(table_bm, targetScaleFactor).generate();
        options.setOptimize(true);
        options.setDumpToFile(false);
    }
    void run() {
        double sumCompilation = 0;
        double sumExecution = 0;
        double sumPythonCompilationShipDate = 0;
        double sumPythonCompilationDiscount = 0;
        double sumPythonCompilationQuantity = 0;
        double sumPythonExecutionShipDate = 0;
        double sumPythonExecutionDiscount = 0;
        double sumPythonExecutionQuantity = 0;
        for (uint64_t i = 0; i < iterations; i++) {
            Timer compileTimeTimer("Compilation");
            Timer executionTimeTimer("Execution");
            runQuery(compileTimeTimer, executionTimeTimer, &sumPythonCompilationShipDate, &sumPythonCompilationDiscount, &sumPythonCompilationQuantity,
                     &sumPythonExecutionShipDate, &sumPythonExecutionDiscount, &sumPythonExecutionQuantity);
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

        double totalCompilationTime = (sumCompilation / (double) iterations);
        double totalExecutionTime = (sumExecution / (double) iterations);
        double totalLatencyTime = totalCompilationTime + totalExecutionTime;
        double totalPythonCompilationTimeShipDate = (sumPythonCompilationShipDate / (double) iterations);
        double totalPythonCompilationTimeDiscount = (sumPythonCompilationDiscount / (double) iterations);
        double totalPythonCompilationTimeQuantity = (sumPythonCompilationQuantity / (double) iterations);
        double totalPythonExecutionTimeShipDate = (sumPythonExecutionShipDate / (double) iterations);
        double totalPythonExecutionTimeDiscount = (sumPythonExecutionDiscount / (double) iterations);
        double totalPythonExecutionTimeQuantity = (sumPythonExecutionQuantity / (double) iterations);

        auto executeFile = std::filesystem::current_path().string() + "/dump/measurements_tpc_h.csv";
        std::ofstream outputFileExecute;
        outputFileExecute.open(executeFile, std::ios_base::app);
        outputFileExecute << "tpc_h_query6_multiple" << ", " << compiler << ", " << pythonCompiler << ", " << totalCompilationTime << ", " << totalExecutionTime << ", " << totalLatencyTime << ", " << totalPythonCompilationTimeShipDate << ", " << totalPythonCompilationTimeDiscount << ", " << totalPythonCompilationTimeQuantity << ", " << totalPythonExecutionTimeShipDate << ", " << totalPythonExecutionTimeDiscount << ", " << totalPythonExecutionTimeQuantity << "\n";
        outputFileExecute.close();
    };

    virtual ~BenchmarkRunnerMultipleTimer() = default;
    virtual void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, double* sumPythonCompilationShipDate, double* sumPythonCompilationDiscount, double* sumPythonCompilationQuantity, double* sumPythonExecutionShipDate, double* sumPythonExecutionDiscount, double* sumPythonExecutionQuantity) = 0;

  protected:
    uint64_t iterations = 10;
    TPCH_Scale_Factor targetScaleFactor = TPCH_Scale_Factor::F1;
    std::string compiler;
    std::string pythonCompiler;
    ExecutablePipelineProvider* provider;
    Nautilus::CompilationOptions options;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::BufferManager> table_bm;
    std::shared_ptr<WorkerContext> wc;
    std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>> tables;
};

class Query6Runner : public BenchmarkRunner {
  public:
    Query6Runner(TPCH_Scale_Factor targetScaleFactor, std::string compiler,  std::string pythonCompiler) : BenchmarkRunner(targetScaleFactor, compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, double* sumPythonCompilation, double* sumPythonExecution) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto plan = TPCH_Query6_Python::getPipelinePlan(tables, bm, pythonCompiler);
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        auto emitExecutablePipeline = provider->create(pipeline2.pipeline, options);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        emitExecutablePipeline->setup(*pipeline2.ctx);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();
        executionTimeTimer.start();
        for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }
        executionTimeTimer.snapshot("execute agg");
        auto dummyBuffer = NES::Runtime::TupleBuffer();
        emitExecutablePipeline->execute(dummyBuffer, *pipeline2.ctx, *wc);
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
        emitExecutablePipeline->stop(*pipeline2.ctx);

        auto handler = pipeline1.ctx->getOperatorHandler<PythonUDFOperatorHandler>(1);

        double sumPythonExec = *sumPythonExecution;
        double sumPythonComp = *sumPythonCompilation;
        sumPythonExec += handler->getSumExecution();
        sumPythonComp += handler->getCompilationTime();
        NES_INFO("sumPython {}, {}", sumPythonComp, sumPythonExec);

        *sumPythonExecution = sumPythonExec;
        *sumPythonCompilation = sumPythonComp;
    }
};

class Query6RunnerFlatMap : public BenchmarkRunner {
  public:
    Query6RunnerFlatMap(TPCH_Scale_Factor targetScaleFactor, std::string compiler,  std::string pythonCompiler) : BenchmarkRunner(targetScaleFactor, compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, double* sumPythonCompilation, double* sumPythonExecution) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto plan = TPCH_Query6_Python_FlatMap::getPipelinePlan(tables, bm, pythonCompiler);
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();
        executionTimeTimer.start();
        auto& chunk = lineitems->getChunks().front();
        //for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        //}
        executionTimeTimer.snapshot("execute agg");
        auto dummyBuffer = NES::Runtime::TupleBuffer();
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);

        auto handler = pipeline1.ctx->getOperatorHandler<PythonUDFOperatorHandler>(0);
        double sumPythonExec = *sumPythonExecution;
        double sumPythonComp = *sumPythonCompilation;
        sumPythonExec += handler->getSumExecution();
        sumPythonComp += handler->getCompilationTime();
        NES_INFO("sumPython {}, {}", sumPythonComp, sumPythonExec);

        *sumPythonExecution = sumPythonExec;
        *sumPythonCompilation = sumPythonComp;
    }
};

class Query6RunnerPandas : public BenchmarkRunner {
  public:
    Query6RunnerPandas(TPCH_Scale_Factor targetScaleFactor, std::string compiler,  std::string pythonCompiler) : BenchmarkRunner(targetScaleFactor, compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, double* sumPythonCompilation, double* sumPythonExecution) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto plan = TPCH_Query6_Python_Pandas::getPipelinePlan(tables, bm, pythonCompiler);
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        auto emitExecutablePipeline = provider->create(pipeline2.pipeline, options);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        emitExecutablePipeline->setup(*pipeline2.ctx);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();
        executionTimeTimer.start();
        for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }

        executionTimeTimer.snapshot("execute agg");
        auto dummyBuffer = NES::Runtime::TupleBuffer();
        emitExecutablePipeline->execute(dummyBuffer, *pipeline2.ctx, *wc);
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
        emitExecutablePipeline->stop(*pipeline2.ctx);

        auto handler = pipeline1.ctx->getOperatorHandler<PythonUDFOperatorHandler>(1);
        double sumPythonExec = *sumPythonExecution;
        double sumPythonComp = *sumPythonCompilation;
        sumPythonExec += handler->getSumExecution();
        sumPythonComp += handler->getCompilationTime();
        NES_INFO("sumPython {}, {}", sumPythonComp, sumPythonExec);

        *sumPythonExecution = sumPythonExec;
        *sumPythonCompilation = sumPythonComp;
    }
};

class Query6RunnerMultipleUDFs : public BenchmarkRunnerMultipleTimer {
  public:
    Query6RunnerMultipleUDFs(TPCH_Scale_Factor targetScaleFactor, std::string compiler,  std::string pythonCompiler) : BenchmarkRunnerMultipleTimer(targetScaleFactor, compiler, pythonCompiler){};
    void runQuery(Timer<>& compileTimeTimer, Timer<>& executionTimeTimer, double* sumPythonCompilationShipDate, double* sumPythonCompilationDiscount, double* sumPythonCompilationQuantity, double* sumPythonExecutionShipDate, double* sumPythonExecutionDiscount, double* sumPythonExecutionQuantity) override {
        auto& lineitems = tables[TPCHTable::LineItem];
        auto plan = TPCH_Query6_Python_Multiple_UDFs::getPipelinePlan(tables, bm, pythonCompiler);
        compileTimeTimer.start();
        auto pipeline1 = plan.getPipeline(0);
        auto pipeline2 = plan.getPipeline(1);
        auto aggExecutablePipeline = provider->create(pipeline1.pipeline, options);
        auto emitExecutablePipeline = provider->create(pipeline2.pipeline, options);
        aggExecutablePipeline->setup(*pipeline1.ctx);
        emitExecutablePipeline->setup(*pipeline2.ctx);
        compileTimeTimer.snapshot("setup");
        compileTimeTimer.pause();
        executionTimeTimer.start();
        /*for (auto& chunk : lineitems->getChunks()) {
            aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);
        }*/
        auto& chunk = lineitems->getChunks().front();
        aggExecutablePipeline->execute(chunk, *pipeline1.ctx, *wc);

        executionTimeTimer.snapshot("execute agg");
        auto dummyBuffer = NES::Runtime::TupleBuffer();
        emitExecutablePipeline->execute(dummyBuffer, *pipeline2.ctx, *wc);
        executionTimeTimer.snapshot("execute emit");
        executionTimeTimer.pause();
        aggExecutablePipeline->stop(*pipeline1.ctx);
        emitExecutablePipeline->stop(*pipeline2.ctx);

        auto handlerShipDate = pipeline1.ctx->getOperatorHandler<PythonUDFOperatorHandler>(1);
        *sumPythonExecutionShipDate += handlerShipDate->getSumExecution();
        *sumPythonCompilationShipDate += handlerShipDate->getCompilationTime();
        NES_INFO("sumPython {}, {}", *sumPythonCompilationShipDate, *sumPythonExecutionShipDate);

        auto handlerDiscount = pipeline1.ctx->getOperatorHandler<PythonUDFOperatorHandler>(2);
        *sumPythonExecutionDiscount += handlerDiscount->getSumExecution();
        *sumPythonCompilationDiscount += handlerDiscount->getCompilationTime();
        NES_INFO("sumPython {}, {}", *sumPythonExecutionDiscount, *sumPythonCompilationDiscount);

        auto handlerQuantity = pipeline1.ctx->getOperatorHandler<PythonUDFOperatorHandler>(3);
        *sumPythonExecutionQuantity += handlerQuantity->getSumExecution();
        *sumPythonCompilationQuantity += handlerQuantity->getCompilationTime();
        NES_INFO("sumPython {}, {}", *sumPythonExecutionQuantity, *sumPythonCompilationQuantity);
    }
};
}// namespace NES::Runtime::Execution

int main(int, char**) {
    NES::TPCH_Scale_Factor targetScaleFactor = NES::TPCH_Scale_Factor::F1;
    std::vector<std::string> compilers = {"PipelineCompiler","CPPPipelineCompiler"};
    //std::vector<std::string> pythonCompilers = {"cpython", "cython", "nuitka"};

    std::vector<std::string> pythonCompilers = {"cpython", "cython", "nuitka", "numba", "pypy"};
    for (const auto& c : compilers) {
        for (const auto& pythonCompiler : pythonCompilers) {
            NES::Runtime::Execution::Query6Runner(targetScaleFactor, c, pythonCompiler).run();
            //NES::Runtime::Execution::Query6RunnerFlatMap(targetScaleFactor, c, pythonCompiler).run();
            //NES::Runtime::Execution::Query6RunnerPandas(targetScaleFactor, c, pythonCompiler).run();
            //NES::Runtime::Execution::Query6RunnerMultipleUDFs(targetScaleFactor, c, pythonCompiler).run();
        }
    }
}