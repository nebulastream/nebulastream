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

namespace NES::EncoderAutoChoice
{
/// Disclaimer: These values are very rough estimates and might vary strongly between workloads and machines
/// They serve as the missing variables in this prototype for a codec selector
inline const double LZ4_RATIO_ESTIMATE = 0.658;
inline const double ZSTD_RATIO_ESTIMATE = 0.502;
inline const double SNAPPY_RATIO_ESTIMATE = 0.622;

inline const double LZ4_RATE_ESTIMATE = 789;
inline const double ZSTD_RATE_ESTIMATE = 227;
inline const double SNAPPY_RATE_ESTIMATE = 806;

std::string chooseCodec(const double& bandwidth, const uint64_t& numberOfWorkerThreads);
}
