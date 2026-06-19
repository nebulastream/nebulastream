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
#include <limits>
#include <Identifiers/NESStrongType.hpp>

namespace NES
{

/// Strong type for a streaming interval-join bound: a signed millisecond offset added to an anchor
/// timestamp to delimit the partner-matching interval [anchorTs + lowerBound, anchorTs + upperBound].
/// Bounds may be negative (past-anchored). Wrapping the raw int64_t stops the lower/upper bounds from
/// being silently swapped or mixed with an unrelated integer; unwrap with getRawValue() for arithmetic.
/// INVALID/INITIAL are placeholders required by NESStrongType and carry no domain meaning for bounds.
using IntervalBound = NESStrongType<std::int64_t, struct IntervalBound_, std::numeric_limits<std::int64_t>::min(), 0>;

}
