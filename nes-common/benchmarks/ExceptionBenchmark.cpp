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

#include <functional>
#include <iostream>
#include <Util/StacktraceLoader.hpp>
#include <benchmark/benchmark.h>
#include <cpptrace.hpp>

/// This Benchmark shows that collecting and resolving a stacktrace is
/// about 100 times slower if the result is cached. If the result is not
/// cached resolved the stacktrace is 40000 times slower.
/// The size of the stack does impact the time of collecting a raw stack trace
/// and limiting the stack trace helps to improve performance if the stack trace
/// would succeed the limit

template <size_t StackDepth>
BENCHMARK_DONT_OPTIMIZE int stack_function(std::function<void()>&& fn)
{
    return stack_function<StackDepth - 1>(std::move(fn));
}

template <>
BENCHMARK_DONT_OPTIMIZE int stack_function<0>(std::function<void()>&& fn)
{
    fn();
    return 0;
}

static void BM_MaterializeStacktraceWithoutWarmUp(benchmark::State& state)
{
    for (auto _ : state)
    {
        stack_function<20>([]() { benchmark::DoNotOptimize(cpptrace::generate_trace(1, 120).to_string()); });
    }
}

static void BM_MaterializeStacktrace(benchmark::State& state)
{
    stack_function<20>([]() { benchmark::DoNotOptimize(cpptrace::generate_trace(1, 120).to_string()); });
    for (auto _ : state)
    {
        stack_function<20>([]() { benchmark::DoNotOptimize(cpptrace::generate_trace(1, 120).to_string()); });
    }
}

static void BM_StackTraceWithMaterialization(benchmark::State& state)
{
    stack_function<20>([]() { benchmark::DoNotOptimize(cpptrace::generate_trace(1, 120).to_string()); });
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(stack_function<20>([]() { benchmark::DoNotOptimize(cpptrace::generate_raw_trace(0).resolve()); }));
    }
}

static void BM_StackTraceWithMaterializationAndStringify(benchmark::State& state)
{
    stack_function<20>([]() { benchmark::DoNotOptimize(cpptrace::generate_trace(1, 120).to_string()); });
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(
            stack_function<20>([]() { benchmark::DoNotOptimize(cpptrace::generate_raw_trace(0).resolve().to_string()); }));
    }
}

template <size_t StackSize>
static void BM_RawStackTrace(benchmark::State& state)
{
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(stack_function<StackSize>([]() { benchmark::DoNotOptimize(cpptrace::generate_raw_trace(0)); }));
    }
}

template <size_t StackSize>
static void BM_RawStackTraceWithLimit(benchmark::State& state)
{
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(
            stack_function<StackSize>([&]() { benchmark::DoNotOptimize(cpptrace::generate_raw_trace(0, state.range(0))); }));
    }
}

/// Register the function as a benchmark
BENCHMARK(BM_MaterializeStacktraceWithoutWarmUp);
BENCHMARK_TEMPLATE(BM_RawStackTrace, 0);
BENCHMARK_TEMPLATE(BM_RawStackTrace, 1);
BENCHMARK_TEMPLATE(BM_RawStackTrace, 5);
BENCHMARK_TEMPLATE(BM_RawStackTrace, 20);
BENCHMARK_TEMPLATE(BM_RawStackTrace, 40);
BENCHMARK_TEMPLATE(BM_RawStackTrace, 80);
BENCHMARK_TEMPLATE(BM_RawStackTrace, 160);
BENCHMARK_TEMPLATE(BM_RawStackTraceWithLimit, 0)->Arg(0);
BENCHMARK_TEMPLATE(BM_RawStackTraceWithLimit, 1)->Range(0, 1);
BENCHMARK_TEMPLATE(BM_RawStackTraceWithLimit, 5)->Range(0, 5);
BENCHMARK_TEMPLATE(BM_RawStackTraceWithLimit, 20)->Range(0, 20);
BENCHMARK_TEMPLATE(BM_RawStackTraceWithLimit, 40)->Range(0, 40);
BENCHMARK_TEMPLATE(BM_RawStackTraceWithLimit, 80)->Range(0, 80);
BENCHMARK_TEMPLATE(BM_RawStackTraceWithLimit, 160)->Range(0, 160);
BENCHMARK(BM_StackTraceWithMaterialization);
BENCHMARK(BM_StackTraceWithMaterializationAndStringify);
BENCHMARK(BM_MaterializeStacktrace);
/// Run the benchmark
BENCHMARK_MAIN();
