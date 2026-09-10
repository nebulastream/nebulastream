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

#include <Identifiers/NESStrongType.hpp>

namespace NES
{

/// Names the payload layout of a stored statistic: the aggregation function that produced it, e.g. "Avg" or
/// "ReservoirSample". A probe compares it against what it expects to decode, so it must not be confused with the
/// many other strings a statistic carries -- field names, sink names, metric labels.
using StatisticBlobType = NESStrongStringType<struct StatisticBlobType_, "INVALID">;

}
