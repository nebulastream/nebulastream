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
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/CSVSource.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <benchmark/benchmark.h>
#include <gmock/gmock.h>

class MockPartitionManager : public NES::Network::PartitionManager {
  public:
    MockPartitionManager() = default;
};

class MockExchangeProtocolListener : public NES::Network::ExchangeProtocolListener {
  public:
    MOCK_METHOD(void, onEvent, (NES::Network::NesPartition, NES::Runtime::BaseEvent&), (override));
    MOCK_METHOD(void, onDataBuffer, (NES::Network::NesPartition, NES::Runtime::TupleBuffer&), (override));
    MOCK_METHOD(void, onEndOfStream, (NES::Network::Messages::EndOfStreamMessage), (override));
    MOCK_METHOD(void, onServerError, (NES::Network::Messages::ErrorMessage), (override));
    MOCK_METHOD(void, onChannelError, (NES::Network::Messages::ErrorMessage), (override));
};

class MockExchangeProtocol : public NES::Network::ExchangeProtocol {
  public:
    MockExchangeProtocol()
        : ExchangeProtocol(std::make_shared<MockPartitionManager>(), std::make_shared<MockExchangeProtocolListener>()) {}
};

class MockNetworkManager : public NES::Network::NetworkManager {
  public:
    MockNetworkManager() : NetworkManager(1, "", static_cast<uint16_t>(9000), MockExchangeProtocol(), nullptr, 0, 0) {}
};

class MockQueryManager final : public NES::Runtime::AbstractQueryManager {
  public:
    MOCK_METHOD(NES::ExecutionResult, processNextTask, (bool running, NES::Runtime::WorkerContext& workerContext), (override));
    MOCK_METHOD(void,
                addWorkForNextPipeline,
                (NES::Runtime::TupleBuffer & buffer,
                 NES::Runtime::Execution::SuccessorExecutablePipeline executable,
                 uint32_t queueId),
                (override));
    MOCK_METHOD(void, poisonWorkers, (), (override));
    MOCK_METHOD(bool,
                addReconfigurationMessage,
                (NES::QueryId queryId,
                 NES::DecomposedQueryPlanId queryExecutionPlanId,
                 const NES::Runtime::ReconfigurationMessage& reconfigurationMessage,
                 bool blocking),
                (override));
    MockQueryManager() : AbstractQueryManager(nullptr, {}, 0, 0, std::make_shared<NES::Runtime::HardwareManager>(), 0, {}) {}

  private:
    MOCK_METHOD(bool,
                addReconfigurationMessage,
                (NES::QueryId queryId,
                 NES::DecomposedQueryPlanId queryExecutionPlanId,
                 NES::Runtime::TupleBuffer&& buffer,
                 bool blocking),
                (override));

  public:
    MOCK_METHOD(uint64_t, getNumberOfTasksInWorkerQueues, (), (const override));

  private:
    bool startThreadPool(uint64_t) override { return true; }

  protected:
    MOCK_METHOD(NES::ExecutionResult, terminateLoop, (NES::Runtime::WorkerContext&), (override));

  public:
    MOCK_METHOD(bool, registerQuery, (const NES::Runtime::Execution::ExecutableQueryPlanPtr& qep), (override));
    MOCK_METHOD(void, postReconfigurationCallback, (NES::Runtime::ReconfigurationMessage & task), (override));
    MOCK_METHOD(void, reconfigure, (NES::Runtime::ReconfigurationMessage&, NES::Runtime::WorkerContext& context), (override));
    MOCK_METHOD(void, destroy, (), (override));

    ~MockQueryManager() override = default;

  protected:
    MOCK_METHOD(void,
                updateStatistics,
                (const NES::Runtime::Task& task,
                 NES::QueryId queryId,
                 NES::DecomposedQueryPlanId subPlanId,
                 NES::PipelineId pipelineId,
                 NES::Runtime::WorkerContext& workerContext),
                (override));
};

class MockQueryStatusListener : public NES::AbstractQueryStatusListener {
  public:
    MOCK_METHOD(bool,
                canTriggerEndOfStream,
                (NES::QueryId queryId,
                 NES::DecomposedQueryPlanId subPlanId,
                 NES::OperatorId sourceId,
                 NES::Runtime::QueryTerminationType),
                (override));
    MOCK_METHOD(bool,
                notifySourceTermination,
                (NES::QueryId queryId,
                 NES::DecomposedQueryPlanId subPlanId,
                 NES::OperatorId sourceId,
                 NES::Runtime::QueryTerminationType),
                (override));
    MOCK_METHOD(bool,
                notifyQueryFailure,
                (NES::QueryId queryId, NES::DecomposedQueryPlanId subQueryId, std::string errorMsg),
                (override));
    MOCK_METHOD(bool,
                notifyQueryStatusChange,
                (NES::QueryId queryId,
                 NES::DecomposedQueryPlanId subQueryId,
                 NES::Runtime::Execution::ExecutableQueryPlanStatus newStatus),
                (override));
    MOCK_METHOD(bool, notifyEpochTermination, (uint64_t timestamp, uint64_t querySubPlanId), (override));
};

class MockNodeEngine : public NES::Runtime::NodeEngine {
    std::shared_ptr<MockQueryStatusListener> queryStatusListener;

  public:
    MockNodeEngine() : MockNodeEngine(std::make_shared<MockQueryStatusListener>()) {}
    MockNodeEngine(std::shared_ptr<MockQueryStatusListener> queryStatusListener)
        : NodeEngine(
              {},
              nullptr,
              {},
              std::make_shared<MockQueryManager>(),
              [](auto /*node*/) {
                  return nullptr;
              },
              nullptr,
              nullptr,
              std::weak_ptr(queryStatusListener),
              nullptr,
              1,
              0,
              100,
              0,
              false),
          queryStatusListener(queryStatusListener) {}
    MOCK_METHOD(void, onFatalError, (int signalNumber, std::string callstack), (override));
    MOCK_METHOD(void, onFatalException, (std::shared_ptr<std::exception> exception, std::string callstack), (override));
    MOCK_METHOD(void, onDataBuffer, (NES::Network::NesPartition, NES::Runtime::TupleBuffer&), (override));
    MOCK_METHOD(void, onEvent, (NES::Network::NesPartition, NES::Runtime::BaseEvent&), (override));
    MOCK_METHOD(void, onEndOfStream, (NES::Network::Messages::EndOfStreamMessage), (override));
    MOCK_METHOD(void, onServerError, (NES::Network::Messages::ErrorMessage), (override));
    MOCK_METHOD(void, onChannelError, (NES::Network::Messages::ErrorMessage), (override));
    ~MockNodeEngine() override = default;
};

class MockDataSink : public NES::SinkMedium {
  public:
    ~MockDataSink() override = default;
    MockDataSink() : SinkMedium(nullptr, std::make_shared<MockNodeEngine>(), 1, 1, 1) {}
    MOCK_METHOD(void, setup, (), (override));
    MOCK_METHOD(void, shutdown, (), (override));
    MOCK_METHOD(bool,
                writeData,
                (NES::Runtime::TupleBuffer & inputBuffer, NES::Runtime::WorkerContext& workerContext),
                (override));
    MOCK_METHOD(std::string, toString, (), (const override));
    MOCK_METHOD(NES::SinkMediumTypes, getSinkMediumType, (), (override));
    MOCK_METHOD(void,
                reconfigure,
                (NES::Runtime::ReconfigurationMessage & message, NES::Runtime::WorkerContext& context),
                (override));
    MOCK_METHOD(void, postReconfigurationCallback, (NES::Runtime::ReconfigurationMessage & message), (override));
};

static void benchMarkCSVSource(benchmark::State& state) {
    using namespace NES;
    std::string filePath;
    NES::Logger::setupLogging("somewhere.log", NES::LogLevel::LOG_INFO);

    auto schema = Schema::create()
                      ->addField("timestamp", BasicType::UINT64)
                      ->addField("value", BasicType::FLOAT32)
                      ->addField("a", BasicType::INT32)
                      ->addField("b", BasicType::INT32)
                      ->addField("c", BasicType::INT32)
                      ->addField("d", BasicType::INT32);
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto query = Query::from("TestSource");

    auto csvSourceType = CSVSourceType::create("TestSource", "TestSourcePhy");
    csvSourceType->setFilePath(fmt::format("{}/{}", BENCHMARK_DATA_DIRECTORY, "smartgrid/smartgrid-data.csv"));
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(0);
    csvSourceType->setSkipHeader(false);

    for (auto _ : state) {
        auto source = std::shared_ptr<CSVSource>(
            new NES::CSVSource(schema,
                               bm,
                               std::make_shared<MockQueryManager>(),
                               csvSourceType,
                               1,
                               1,
                               1,
                               100,
                               GatheringMode::INTERVAL_MODE,
                               "TestSourcePhy",
                               std::vector<Runtime::Execution::SuccessorExecutablePipeline>{std::make_shared<MockDataSink>()}));

        source->open();
        while (source->receiveData().has_value()) {
        }
        source->close();
    }
}

BENCHMARK(benchMarkCSVSource);
BENCHMARK_MAIN();

// int main() {
//     benchMarkCSVSource();
// }
