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
#ifndef NES_UNIKERNELSOURCE_HPP
#define NES_UNIKERNELSOURCE_HPP

#include <Network/NetworkSource.hpp>
#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>
#include <Sources/TCPSource.hpp>

namespace NES::Unikernel {
template<typename Config, typename Prev>
class UnikernelSourceImpl {
  public:
    constexpr static size_t Id = Config::UpstreamNodeID;
    static std::optional<typename Config::SourceType> source;

    static void setup() {
        if constexpr (std::same_as<NES::Network::NetworkSource, typename Config::SourceType>) {
            NES_INFO("Calling Setup for NetworkSource");
            UnikernelSourceImpl::source.emplace(
                NES::Network::NesPartition(Config::QueryID,
                                           Config::UpstreamOperatorID,
                                           Config::UpstreamPartitionID,
                                           Config::UpstreamSubPartitionID),
                NES::Network::NodeLocation(Config::UpstreamNodeID, Config::UpstreamNodeHostname, Config::UpstreamNodePort),
                200ms,
                100,
                UnikernelPipelineExecutionContext::create<Prev>());
            source->bind();
            source->start();
        } else if constexpr (std::same_as<NES::TCPSource<Config>, typename Config::SourceType>) {
            NES_INFO("Calling Setup for TCPSource");
            UnikernelSourceImpl::source.emplace(UnikernelPipelineExecutionContext::create<Prev>());
            source->start();
            source->runningRoutine();
        }
    }

    static void stop() { auto ret = source->stop(Runtime::QueryTerminationType::Graceful); }
};

template<typename Config>
class UnikernelSource {
  public:
    template<typename Prev>
    using Impl = UnikernelSourceImpl<Config, Prev>;
};

template<typename Config, typename Prev>
std::optional<typename Config::SourceType> UnikernelSourceImpl<Config, Prev>::source = std::nullopt;
}// namespace NES::Unikernel

#endif//NES_UNIKERNELSOURCE_HPP
