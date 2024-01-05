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
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <TestUtils/BasicTraceFunctions.hpp>
#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus::Tracing {

#define BENCHMARK_TRACE_FUNCTION(TARGET_FUNCTION)                                                                                \
    static void TARGET_FUNCTION(benchmark::State& state) {                                                                       \
        for (auto _ : state) {                                                                                                   \
            auto executionTrace = Nautilus::Tracing::traceFunction([]() {                                                        \
                TARGET_FUNCTION();                                                                                               \
            });                                                                                                                  \
        }                                                                                                                        \
    }                                                                                                                            \
    BENCHMARK(TARGET_FUNCTION)->Unit(benchmark::kMillisecond);

BENCHMARK_TRACE_FUNCTION(assignmentOperator);
BENCHMARK_TRACE_FUNCTION(arithmeticExpression);
BENCHMARK_TRACE_FUNCTION(logicalExpression);
BENCHMARK_TRACE_FUNCTION(nestedIfThenElseCondition);
BENCHMARK_TRACE_FUNCTION(emptyLoop);
BENCHMARK_TRACE_FUNCTION(longEmptyLoop);
BENCHMARK_TRACE_FUNCTION(sumLoop);
BENCHMARK_TRACE_FUNCTION(sumWhileLoop);
BENCHMARK_TRACE_FUNCTION(invertedLoop);
BENCHMARK_TRACE_FUNCTION(nestedLoop);
BENCHMARK_TRACE_FUNCTION(nestedLoopIf);
BENCHMARK_TRACE_FUNCTION(loopWithBreak);
BENCHMARK_TRACE_FUNCTION(f1);
BENCHMARK_TRACE_FUNCTION(TracingBreaker);
BENCHMARK_TRACE_FUNCTION(deepLoop);

}// namespace NES::Nautilus::Tracing

BENCHMARK_MAIN();