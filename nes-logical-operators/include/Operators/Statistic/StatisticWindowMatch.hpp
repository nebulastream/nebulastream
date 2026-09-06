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

#include <cstdint>

namespace NES
{

/// How a probe selects stored statistics for the window bounds it is given.
///
/// A probe chained onto a build sees one exact window per record and wants that one statistic. A probe driven by
/// an impulse over a time range wants every window falling inside it, so the same bounds mean something different.
enum class StatisticWindowMatch : uint8_t
{
    ExactWindow,
    WithinRange
};

}
