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


#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <API/Schema.hpp>
#include <Async/AsyncSourceRunner.hpp>
#include <Async/Sources/TCPSource.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceExecutionContext.hpp>
#include <Sources/SourceRunner.hpp>
#include <Sources/SourceUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::Sources::Benchmarks
{

template <typename FuncT, typename DurationT>
auto measure(FuncT&& func, DurationT* durationOut)
{
    const auto start = std::chrono::steady_clock::now();
    if constexpr (std::is_same_v<void, std::invoke_result_t<FuncT>>)
    {
        func();
        *durationOut = std::chrono::duration_cast<DurationT>(std::chrono::steady_clock::now() - start);
    }
    else
    {
        auto return_value = func();
        *durationOut = std::chrono::duration_cast<DurationT>(std::chrono::steady_clock::now() - start);
        return return_value;
    }
}

struct BenchmarkOptions
{
    auto bufferSize = 4'096; // bytes
    auto numBuffers = 1'048'576; // 4 GiB buffer pool
    auto numBuffersPerSource = 1;
    auto tcpHost = "localhost";
    uint32_t tcpPort = 12345;
    auto benchmarkRepetitions = 5;
    auto numSources = 1;
    auto multiplier = 10;
    auto duration = std::chrono::seconds(10);
};

struct BenchmarkStatistics
{
    uint64_t processedBuffers = {0}; // total
    uint64_t processedBytes{0}; // total
    uint64_t throughputInBytesPerSecond{0};
    std::chrono::milliseconds startupTime;
    std::chrono::milliseconds shutdownTime;
};

using IOBuffer = Memory::TupleBuffer;

struct NoOpInputFormatter : InputFormatters::InputFormatter
{
    void parseTupleBufferRaw(
        const IOBuffer& tbRaw,
        Memory::AbstractBufferProvider& /*bufferProvider*/,
        size_t,
        const std::function<void(IOBuffer& buffer)>& emitFunction) override
    {
        emitFunction(const_cast<IOBuffer&>(tbRaw));
    }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& os) const override { return os << "NoOpInputFormatter"; }
};

class Benchmark
{
public:
    explicit Benchmark(const BenchmarkOptions& options)
        : bufferManager{Memory::BufferManager::create(options.bufferSize, options.numBuffers)}, options(options)
    {
    }

    void run()
    {
        for (size_t i = 0; i < options.benchmarkRepetitions; ++i)
        {
            initialize();
            measure(start, &statistics.startupTime);
            waitFor();
            measure(stop, &statistics.shutdownTime);
            statistics.processedBytes = statistics.processedBuffers * options.bufferSize;
            statistics.throughputInBytesPerSecond = statistics.processedBytes / std::chrono::duration_cast<std::chrono::seconds>(options.duration).count();
            reset();
        }
    }

    void start()
    {
        for (const auto& source : sources)
        {
            source->start(emitFunction());
        }
    }

    void stop()
    {
        for (const auto& source : sources)
        {
            source->stop();
        }
    }

    void waitFor() const { std::this_thread::sleep_for(options.duration); }

    BenchmarkStatistics getStatistics() const { return statistics; }

private:
    std::shared_ptr<Memory::BufferManager> bufferManager;
    std::vector<std::unique_ptr<SourceRunner>> sources;
    BenchmarkStatistics statistics;
    BenchmarkOptions options;


    void initialize()
    {
        sources.reserve(options.numSources);

        const std::shared_ptr<Schema> schema = Schema::create()->addField("id", BasicType::INT32);
        const ParserConfig parserConfig = {.parserType = "", .tupleDelimiter = "", .fieldDelimiter = ""};
        const SourceDescriptor descriptor{
            schema, "benchmarkTcpSource", "TCP", parserConfig, {{"socketHost", options.tcpHost}, {"socketPort", options.tcpPort}}};

        for (size_t i = 0; i < options.numSources; ++i)
        {
            auto sourceImpl = std::make_unique<TCPSource>(descriptor);
            auto bufferProvider = bufferManager->createFixedSizeBufferPool(options.numBuffersPerSource).value();
            auto formatter = std::make_unique<NoOpInputFormatter>();
            auto sourceContext = SourceExecutionContext{OriginId{i}, std::move(sourceImpl), bufferProvider, std::move(formatter)};

            auto runner = std::make_unique<AsyncSourceRunner>(std::move(sourceContext));
            sources.push_back(std::move(runner));
        }
    }

    void reset()
    {
        sources.clear();
    }


    EmitFunction emitFunction()
    {
        return [this](OriginId, SourceReturnType event)
        { std::visit(Overloaded{[&](Data) { statistics.processedBuffers++; }, [&](EoS) {}, [&](const Error&) {}}, std::move(event)); };
    }
};

};

int main()
{
}