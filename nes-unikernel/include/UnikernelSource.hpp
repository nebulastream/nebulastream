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

#include <Network/UnikernelNetworkSource.hpp>
#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/TCPSource.hpp>

namespace NES::Unikernel {
template<typename Config, typename Prev>
class UnikernelSourceImpl {
  public:
    constexpr static size_t Id = Config::OperatorId;
    using DataSourceType = DataSource<Config, typename Config::SourceType>;
    static DataSourceType source;

    static void setup() { source.start(); }

    static void stop(Runtime::QueryTerminationType type) {
        NES_INFO("Pipeline Stop called on Source{}", Id);
        source.stop(type);
    }
    static void request_stop(Runtime::QueryTerminationType type) {
        NES_INFO("Pipeline Stop requested on Source{}", Id);
        source.stop(type);
    }
};

template<typename Config>
class UnikernelSource {
  public:
    template<typename Prev>
    using Impl = UnikernelSourceImpl<Config, Prev>;
};

template<typename Config, typename Prev>
typename UnikernelSourceImpl<Config, Prev>::DataSourceType
    UnikernelSourceImpl<Config, Prev>::source(UnikernelPipelineExecutionContext::create<Prev>());
}// namespace NES::Unikernel

#endif//NES_UNIKERNELSOURCE_HPP
