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
#ifndef NES_UNIKERNELSINK_HPP
#define NES_UNIKERNELSINK_HPP

#include <Network/NetworkManager.hpp>
#include <Network/NetworkSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
extern NES::Runtime::WorkerContextPtr TheWorkerContext;
namespace {
using namespace std::chrono_literals;

template<typename SubQuery, typename Config>
class UnikernelSinkImpl {
  public:
    static constexpr size_t StageId = 1;
    static std::optional<typename Config::SinkType> sink;

    static void setup() {
        if constexpr (std::is_same_v<typename Config::SinkType, NES::Network::NetworkSink>) {
            NES_INFO("Calling Setup for NetworkSink");
            UnikernelSinkImpl::sink.emplace(
                1,
                Config::QueryID,
                Config::QuerySubplanID,
                NES::Network::NodeLocation(Config::DownstreamNodeID, Config::DownstreamNodeHostname, Config::DownstreamNodePort),
                NES::Network::NesPartition(Config::QueryID,
                                           Config::DownstreamOperatorID,
                                           Config::DownstreamPartitionID,
                                           Config::DownstreamSubPartitionID),
                Config::OutputSchemaSizeInBytes,
                1,
                200ms,
                100);
            sink->preSetup();
            sink->setup();
        } else if constexpr (std::is_same_v<typename Config::SinkType, NES::KafkaSink<Config>>) {
            NES_INFO("Calling Setup for KafkaSink");
            UnikernelSinkImpl::sink.emplace(1, Config::Broker, Config::Topic, Config::QueryID, Config::QuerySubplanID);
        } else {
            // Test Sink
            UnikernelSinkImpl::sink.emplace(1);
        }
    }

    static void execute(NES::Runtime::TupleBuffer& tupleBuffer) { sink->writeData(tupleBuffer, *TheWorkerContext); }

    static void stop(size_t /*stage_id*/, NES::Runtime::QueryTerminationType type) {
        NES_INFO("Calling Stop for Sink {}", StageId);
        sink->shutdown();
        SubQuery::stop(type);
    }

    static void request_stop(NES::Runtime::QueryTerminationType /*type*/) { sink->shutdown(); }
};

template<typename Config>
class UnikernelSink {
  public:
    template<typename SubQuery>
    using Impl = UnikernelSinkImpl<SubQuery, Config>;
};
template<typename SubQuery, typename Config>
std::optional<typename Config::SinkType> UnikernelSinkImpl<SubQuery, Config>::sink = std::nullopt;
}// namespace

#endif//NES_UNIKERNELSINK_HPP
