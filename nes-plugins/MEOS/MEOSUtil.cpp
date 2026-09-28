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
#include <MEOSUtil.hpp>

namespace NES
{
MEOS::Meos::TemporalSequence constructTemporalSequence(const int8_t* seqAddress, const uint64_t& seqElements)
{
    std::vector<MEOS::Meos::TemporalInstant> tInstantVector;
    tInstantVector.reserve(seqElements);
    tInstantVector.reserve(seqElements);
    /// A single temporal point consists of the doubles for lon and lat, and a uint64 for the timestamp
    constexpr size_t temporalPointSize = sizeof(double) * 2 + sizeof(uint64_t);
    for (size_t i = 0; i < seqElements; ++i)
    {
        const double lon = *reinterpret_cast<const double*>(seqAddress + (i * temporalPointSize));
        const double lat = *reinterpret_cast<const double*>(seqAddress + (i * temporalPointSize) + sizeof(double));
        const uint64_t ts = *reinterpret_cast<const uint64_t*>(seqAddress + (i * temporalPointSize) + (sizeof(double) * 2));
        tInstantVector.push_back(MEOS::Meos::TemporalInstant(lon, lat, ts));
    }
    /// Create a seperate vector for pointers to the tInstants
    std::vector<MEOS::Meos::TemporalInstant*> tInstantAddresses;
    tInstantAddresses.reserve(seqElements);
    for (MEOS::Meos::TemporalInstant& tInstant : tInstantVector)
    {
        tInstantAddresses.push_back(&tInstant);
    }
    return MEOS::Meos::TemporalSequence{tInstantAddresses};
}
}
