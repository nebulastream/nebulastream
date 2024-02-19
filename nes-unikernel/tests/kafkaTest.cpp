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

#include <Sources/KafkaSource.hpp>
#include <UnikernelExecutionPlan.hpp>
#include <UnikernelStage.hpp>

namespace NES::Unikernel {
struct TestSinkImpl {
    NES::OperatorId operatorId;
    void writeData(NES::Runtime::TupleBuffer&, NES::Runtime::WorkerContext&) { NES_INFO("Sink Received Data"); }
    void shutdown() { NES_INFO("Sink was shutdown"); }
};
struct KafkaSinkConfig {
    constexpr static const char* Broker = "localhost:9092";
    constexpr static const char* Topic = "nexmark-bid-csv-result";
    constexpr static size_t QueryId = 1;
    constexpr static size_t QuerySubplanId = 1;
    using Schema = Schema<Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<FLOAT64>>;
    using SinkType = KafkaSink<KafkaSinkConfig>;
};

struct KafkaSourceConfig {
    constexpr static bool CopyBuffer = true;
    constexpr static size_t OriginId = 1;

    constexpr static size_t LocalBuffers = 100;
    constexpr static size_t OperatorId = 1;
    constexpr static const char* Broker = "localhost:9092";
    constexpr static const char* Topic = "nexmark-bid-csv";
    constexpr static int GroupId = 1;
    constexpr static size_t BatchSize = 2;
    constexpr static std::chrono::milliseconds BufferFlushInterval = 100ms;
    using Schema = Schema<Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<UINT64>, Field<FLOAT64>>;
    using SourceType = KafkaSource<KafkaSourceConfig>;
};

static_assert(DataSourceImpl<KafkaSource<KafkaSourceConfig>, KafkaSourceConfig>, "KafkaSource does not implement DataSourceImpl");

using Sink = UnikernelSink<KafkaSinkConfig>;
using Source = UnikernelSource<KafkaSourceConfig>;
using SubQueryPlan = SubQuery<Sink, Pipeline<Source>>;
using QueryPlan = UnikernelExecutionPlan<SubQueryPlan>;

}// namespace NES::Unikernel

NES::Runtime::WorkerContextPtr TheWorkerContext = nullptr;
NES::Network::NetworkManagerPtr TheNetworkManager = nullptr;
NES::Runtime::BufferManagerPtr TheBufferManager = nullptr;
int main() {
    NES::Logger::setupLogging(NES::LogLevel::LOG_DEBUG);
    TheBufferManager = std::make_shared<NES::Runtime::BufferManager>();
    TheBufferManager->createFixedSizeBufferPool(128);
    TheWorkerContext = new NES::Runtime::WorkerContext(0, TheBufferManager, 1, 1);
    NES::Unikernel::QueryPlan::setup();
    NES::Unikernel::QueryPlan::wait();
    return 0;
}
