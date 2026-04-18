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

#pragma once
#include <string>
#include <cstdint>
#include <unordered_map>

namespace NES::EncoderAutoChoice
{
/// Disclaimer: These values are very rough estimates and might vary strongly between workloads and machines
/// They serve as the missing variables in this prototype for a codec selector but are not supposed to be taken as a "guarantee" for satifying compression rates and ratios
inline const std::unordered_map<uint64_t, double> LZ4_RATIO_ESTIMATE{{16384, 0.646}, {32768, 0.642}, {65536, 0.63}};
inline const std::unordered_map<uint64_t, double> ZSTD_RATIO_ESTIMATE{{16384, 0.491}, {32768, 0.491}, {65536, 0.473}};
inline const std::unordered_map<uint64_t, double> SNAPPY_RATIO_ESTIMATE {{16384, 0.61}, {32768, 0.607}, {65536, 0.604}};

inline const std::unordered_map<uint64_t, double> LZ4_RATE_ESTIMATE {{16384, 799}, {32768, 840}, {65536, 789}};
inline const std::unordered_map<uint64_t, double> ZSTD_RATE_ESTIMATE {{16384, 300}, {32768, 324}, {65536, 299}};
inline const std::unordered_map<uint64_t, double> SNAPPY_RATE_ESTIMATE{{16384, 983}, {32768, 806}, {65536, 937}};

std::string chooseCodec(const double& bandwidth, const uint64_t& numberOfWorkerThreads, const uint64_t& bufferSize);
}
