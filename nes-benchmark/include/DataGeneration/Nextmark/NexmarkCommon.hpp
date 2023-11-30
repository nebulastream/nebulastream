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
#ifndef NES_BENCHMARK_INCLUDE_DATAGENERATION_NEXTMARK_NEXMARKCOMMON_HPP_
#define NES_BENCHMARK_INCLUDE_DATAGENERATION_NEXTMARK_NEXMARKCOMMON_HPP_
namespace NES::Benchmark::DataGeneration {

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

}// namespace NES::Benchmark::DataGeneration
#endif// NES_BENCHMARK_INCLUDE_DATAGENERATION_NEXTMARK_NEXMARKCOMMON_HPP_
