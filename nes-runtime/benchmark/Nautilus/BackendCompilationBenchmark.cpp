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
#include "1000_test.cpp"
#include "100_test.cpp"
#include "10_test.cpp"
#include "1100_test.cpp"
#include "1200_test.cpp"
#include "1300_test.cpp"
#include "1400_test.cpp"
#include "1500_test.cpp"
#include "1_test.cpp"
#include "200_test.cpp"
#include "300_test.cpp"
#include "400_test.cpp"
#include "500_test.cpp"
#include "600_test.cpp"
#include "700_test.cpp"
#include "800_test.cpp"
#include "900_test.cpp"
#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <benchmark/benchmark.h>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <utility>
namespace NES::Nautilus {

int32_t predicate(int32_t, int32_t) { return true; }

std::unique_ptr<Nautilus::Backends::Executable> prepare(std::shared_ptr<Nautilus::Tracing::ExecutionTrace> executionTrace,
                                                        std::string compilerName) {
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    Nautilus::IR::RemoveBrOnlyBlocksPhase removeBrOnlyBlocksPhase;
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_DEBUG2("{}", executionTrace.get()->toString());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_DEBUG2("{}", ir->toString());
    auto options = CompilationOptions();
    options.setOptimize(true);
    options.setDebug(false);
    options.setDumpToConsole(false);
    options.setDumpToFile(false);
    auto dumpHelper = DumpHelper::create(compilerName, false, false, "");
    auto& compiler = Backends::CompilationBackendRegistry::getPlugin(std::move(compilerName));
    return compiler->compile(ir, options, dumpHelper);
}

#define BENCHMARK_TRACE_FUNCTION(TARGET_FUNCTION)                                                                                \
    static void TARGET_FUNCTION(benchmark::State& state) {                                                                       \
        Value<Int32> x = Value<Int32>((int32_t) 1);                                                                              \
        x.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0);                                                                       \
        Value<Int32> y = Value<Int32>((int32_t) 1);                                                                              \
        y.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1);                                                                       \
        for (auto _ : state) {                                                                                                   \
            auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([x, y]() {                                          \
                return Nautilus::TARGET_FUNCTION(x, y);                                                                          \
            });                                                                                                                  \
        }                                                                                                                        \
    }                                                                                                                            \
    BENCHMARK(TARGET_FUNCTION)->Unit(benchmark::kMillisecond)->Iterations(10);

BENCHMARK_TRACE_FUNCTION(test_1);
BENCHMARK_TRACE_FUNCTION(test_10);
BENCHMARK_TRACE_FUNCTION(test_100);
BENCHMARK_TRACE_FUNCTION(test_200);
BENCHMARK_TRACE_FUNCTION(test_300);
BENCHMARK_TRACE_FUNCTION(test_400);
BENCHMARK_TRACE_FUNCTION(test_500);
BENCHMARK_TRACE_FUNCTION(test_600);
BENCHMARK_TRACE_FUNCTION(test_700);
BENCHMARK_TRACE_FUNCTION(test_800);
BENCHMARK_TRACE_FUNCTION(test_900);
BENCHMARK_TRACE_FUNCTION(test_1000);
BENCHMARK_TRACE_FUNCTION(test_1100);
BENCHMARK_TRACE_FUNCTION(test_1200);
BENCHMARK_TRACE_FUNCTION(test_1300);
BENCHMARK_TRACE_FUNCTION(test_1400);
BENCHMARK_TRACE_FUNCTION(test_1500);

#define BENCHMARK_COMPILATION_FUNCTION(TARGET_FUNCTION, COMPILER)                                                                \
    static void _##TARGET_FUNCTION##_##COMPILER##_(benchmark::State& state) {                                                    \
        NES::Logger::setupLogging("IfCompilationTest.log", NES::LogLevel::LOG_NONE);                                             \
        Value<Int32> x = Value<Int32>((int32_t) 1);                                                                              \
        x.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0);                          \
        Value<Int32> y = Value<Int32>((int32_t) 1);                                                                              \
        y.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 1);                          \
                                                                                                                                 \
        for (auto _ : state) {                                                                                                   \
            auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([x, y]() {                                          \
                return Nautilus::TARGET_FUNCTION(x, y);                                                                          \
            });                                                                                                                  \
            auto engine = prepare(executionTrace, #COMPILER);                                                                    \
            auto function = engine->getInvocableMember<int32_t, int32_t, int32_t>("execute");                                    \
        }                                                                                                                        \
    }                                                                                                                            \
    BENCHMARK(_##TARGET_FUNCTION##_##COMPILER##_)->Iterations(10)->Unit(benchmark::kMillisecond);

BENCHMARK_COMPILATION_FUNCTION(test_1, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_10, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_100, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_200, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_300, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_400, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_500, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_600, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_700, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_800, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_900, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_1000, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_1100, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_1200, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_1300, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_1400, CPPCompiler);
BENCHMARK_COMPILATION_FUNCTION(test_1500, CPPCompiler);

BENCHMARK_COMPILATION_FUNCTION(test_1, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_10, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_100, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_200, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_300, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_400, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_500, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_600, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_700, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_800, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_900, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_1000, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_1100, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_1200, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_1300, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_1400, Flounder);
BENCHMARK_COMPILATION_FUNCTION(test_1500, Flounder);

BENCHMARK_COMPILATION_FUNCTION(test_1, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_10, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_100, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_200, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_300, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_400, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_500, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_600, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_700, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_800, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_900, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_1000, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_1100, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_1200, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_1300, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_1400, MLIR);
BENCHMARK_COMPILATION_FUNCTION(test_1500, MLIR);

BENCHMARK_COMPILATION_FUNCTION(test_1, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_10, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_100, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_200, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_300, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_400, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_500, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_600, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_700, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_800, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_900, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_1000, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_1100, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_1200, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_1300, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_1400, MIR);
BENCHMARK_COMPILATION_FUNCTION(test_1500, MIR);

BENCHMARK_COMPILATION_FUNCTION(test_1, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_10, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_100, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_200, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_300, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_400, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_500, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_600, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_700, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_800, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_900, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_1000, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_1100, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_1200, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_1300, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_1400, BCInterpreter);
BENCHMARK_COMPILATION_FUNCTION(test_1500, BCInterpreter);

// Tests all registered compilation backends.
// To select a specific compilation backend use ::testing::Values("MLIR") instead of ValuesIn.
/*INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        BackendCompilationBenchmark,
                        ::testing::ValuesIn(Backends::CompilationBackendRegistry::getPluginNames().begin(),
                                            Backends::CompilationBackendRegistry::getPluginNames().end()),
                        [](const testing::TestParamInfo<BackendCompilationBenchmark::ParamType>& info) {
                            return info.param;
                        });
                        */

}// namespace NES::Nautilus

BENCHMARK_MAIN();