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

namespace NES::Unikernel {
template<typename Config>
class UnikernelSource {
  public:
    constexpr static size_t Id = Config::UpstreamNodeID;

    static std::optional<NES::Network::NetworkSource> source;
    static void setup(UnikernelPipelineExecutionContext ctx) {
        UnikernelSource::source.emplace(
            NES::Network::NesPartition(Config::QueryID,
                                       Config::UpstreamOperatorID,
                                       Config::UpstreamPartitionID,
                                       Config::UpstreamSubPartitionID),
            NES::Network::NodeLocation(Config::UpstreamNodeID, Config::UpstreamNodeHostname, Config::UpstreamNodePort),
            Config::LocalBuffers,
            200ms,
            100,
            ctx,
            "Upstream");
    }

    static void start() {
        source->bind();
        source->start();
    }

    static void stop() { source->stop(); }
};
template<typename T>
std::optional<NES::Network::NetworkSource> UnikernelSource<T>::source = std::nullopt;
}// namespace NES::Unikernel

#endif//NES_UNIKERNELSOURCE_HPP
