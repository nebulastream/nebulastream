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

#include "TestSource.hpp"
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <UnikernelExecutionPlan.hpp>

NES::Runtime::BufferManagerPtr TheBufferManager = nullptr;
NES::Runtime::WorkerContextPtr TheWorkerContext = nullptr;

#define NEXMARK_BID_GENERATOR
#ifdef NEXMARK_BID_GENERATOR
class NexmarkCommon {
  public:
    static constexpr long PERSON_EVENT_RATIO = 1;
    static constexpr long AUCTION_EVENT_RATIO = 4;
    static constexpr long BID_EVENT_RATIO = 4;
    static constexpr long TOTAL_EVENT_RATIO = PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO + BID_EVENT_RATIO;

    static constexpr int MAX_PARALLELISM = 50;

    static const long START_ID_AUCTION[MAX_PARALLELISM];
    static const long START_ID_PERSON[MAX_PARALLELISM];

    static constexpr long MAX_PERSON_ID = 540000000L;
    static constexpr long MAX_AUCTION_ID = 540000000000L;
    static constexpr long MAX_BID_ID = 540000000000L;

    static constexpr int HOT_SELLER_RATIO = 100;
    static constexpr int HOT_AUCTIONS_PROB = 85;
    static constexpr int HOT_AUCTION_RATIO = 100;
};

template<typename Schema, size_t BufferSize>
void fillBuffer(NES::Unikernel::SchemaBuffer<Schema, BufferSize>& tb, int ts) {
    static std::mt19937 dist;
    for (size_t i = 0; i < tb.getCapacity(); i++) {
        long auction, bidder;

        long epoch = i / NexmarkCommon::TOTAL_EVENT_RATIO;

        if (dist() % 100 > NexmarkCommon::HOT_AUCTIONS_PROB) {
            auction = (((epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1)
                        / NexmarkCommon::HOT_AUCTION_RATIO)
                       * NexmarkCommon::HOT_AUCTION_RATIO);
        } else {
            long a = std::max(0L, epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1 - 20000);
            long b = epoch * NexmarkCommon::AUCTION_EVENT_RATIO + NexmarkCommon::AUCTION_EVENT_RATIO - 1;
            auction = a + dist() % (b - a + 1 + 100);
        }

        if (dist() % 100 > 85) {
            long personId = epoch * NexmarkCommon::PERSON_EVENT_RATIO + NexmarkCommon::PERSON_EVENT_RATIO - 1;
            bidder = (personId / NexmarkCommon::HOT_SELLER_RATIO) * NexmarkCommon::HOT_SELLER_RATIO;
        } else {
            long personId = epoch * NexmarkCommon::PERSON_EVENT_RATIO + NexmarkCommon::PERSON_EVENT_RATIO - 1;
            long activePersons = std::min(personId, 60000L);
            long n = dist() % (activePersons + 100);
            bidder = personId + activePersons - n;
        }

        auto currentts = ts++;
        tb.writeTuple(std::make_tuple(currentts, auction, bidder, currentts, 0.1));
    }
}
#endif
void ignore(auto) {}
#define NUMBER_OF_BUFFERS 10000
int main() {
    NES::Logger::setupLogging(static_cast<NES::LogLevel>(NES_COMPILE_TIME_LOG_LEVEL));

    errno = 0;
    TheBufferManager = std::make_shared<NES::Runtime::BufferManager>(8192, NUMBER_OF_BUFFERS + 300);
    TheWorkerContext = new NES::Runtime::WorkerContext(NES::Unikernel::CTConfiguration::NodeId, TheBufferManager, 1, 1);

    std::vector<NES::Runtime::TupleBuffer> buffers;

    for (int i = 0; i < NUMBER_OF_BUFFERS; i++) {
        auto buffer = TheBufferManager->getBufferBlocking();
        auto schemaBuffer = NES::Unikernel::SchemaBuffer<typename NES::Unikernel::SourceConfig35::Schema, 8192>::of(buffer);
        fillBuffer(schemaBuffer, i * schemaBuffer.getCapacity());
        buffers.push_back(std::move(buffer));
    }

    NES_INFO("Generation Done");

    std::vector<std::jthread> threads;
    std::apply(
        [&threads, &buffers]<typename... T>(T... t) {
            ((threads.push_back(std::jthread([&buffers] {
                 for (auto& buf : buffers) {
                     NES::Unikernel::SourceHandle<T>->emit(buf);
                 }
                 NES::Unikernel::SourceHandle<T>->eof();
             })),
              ignore(t)),
             ...);
        },
        NES::Unikernel::sources{});

    NES::Unikernel::QueryPlan::setup();
    NES::Unikernel::QueryPlan::wait();
}