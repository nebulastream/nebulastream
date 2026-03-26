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
#include <compare>
#include <ostream>
#include <Identifiers/NESStrongType.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A module-local SequenceNumber used by the input-formatter's SequenceShredder.
/// Constructed from a depth-1 SequenceRange by extracting the top-level component.
using SequenceNumber = NESStrongType<uint64_t, struct SequenceNumberTag_, 0, 1>;

/// Extract a SequenceNumber from a SequenceRange, enforcing depth == 1.
inline SequenceNumber sequenceNumberFromRange(const SequenceRange& range)
{
    PRECONDITION(range.start.depth() == 1, "SequenceNumber requires depth-1 range, got depth {}", range.start.depth());
    return SequenceNumber(range.start[0]);
}

}
