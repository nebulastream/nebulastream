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

#include <Sequencing/SequenceRange.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>
#include <Sequencing/FracturedNumber.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SequenceRange SequenceRange::initial(size_t depth)
{
    std::vector<uint64_t> initialValuesStart(depth, 0);
    std::vector<uint64_t> initialValuesEnd(depth, 0);
    initialValuesEnd[0] = 1;
    return SequenceRange(FracturedNumber(std::move(initialValuesStart)), FracturedNumber(std::move(initialValuesEnd)));
}

SequenceRange SequenceRange::invalid()
{
    return SequenceRange();
}

SequenceRange::SequenceRange(FracturedNumber start, FracturedNumber end) : start(std::move(start)), end(std::move(end))
{
    PRECONDITION(this->start.isValid() && this->end.isValid(), "SequenceRange requires valid start and end");
    PRECONDITION(this->start < this->end, "SequenceRange requires start < end, got {} >= {}", this->start, this->end);
}

bool SequenceRange::isValid() const
{
    return start.isValid() && end.isValid() && start < end;
}

bool SequenceRange::isAdjacentTo(const SequenceRange& other) const
{
    return end == other.start;
}

SequenceRange SequenceRange::merge(const SequenceRange& other) const
{
    PRECONDITION(isAdjacentTo(other), "Cannot merge non-adjacent ranges: {} and {}", toString(), other.toString());
    return SequenceRange(start, other.end);
}

std::vector<SequenceRange> SequenceRange::fracture(size_t n) const
{
    PRECONDITION(n >= 2, "Fracture requires at least 2 sub-ranges, got {}", n);
    PRECONDITION(isValid(), "Cannot fracture an invalid range");

    std::vector<SequenceRange> result;
    result.reserve(n);

    /// Add a sublevel: start becomes start.0, end becomes end.0
    /// Then create n evenly spaced sub-ranges at the new depth
    auto subStart = start.withSubLevel(0);
    auto subEnd = end.withSubLevel(0);

    for (size_t i = 0; i < n; ++i)
    {
        FracturedNumber rangeStart;
        FracturedNumber rangeEnd;

        if (i == 0)
        {
            rangeStart = subStart;
        }
        else
        {
            rangeStart = start.withSubLevel(i);
        }

        if (i == n - 1)
        {
            rangeEnd = subEnd;
        }
        else
        {
            rangeEnd = start.withSubLevel(i + 1);
        }

        result.emplace_back(rangeStart, rangeEnd);
    }

    return result;
}

bool SequenceRange::contains(const FracturedNumber& point) const
{
    return start <= point && point < end;
}

bool SequenceRange::overlap(const SequenceRange& other) const
{
    return
        (start < other.end && other.start < end)
    ||  (other.start < end && start < other.end);
}

bool SequenceRange::covers(const SequenceRange& other) const
{
    return start <= other.start && other.end <= end;
}

std::strong_ordering SequenceRange::operator<=>(const SequenceRange& other) const
{
    return start <=> other.start;
}

bool SequenceRange::operator==(const SequenceRange& other) const
{
    return start == other.start && end == other.end;
}

std::string SequenceRange::toString() const
{
    if (!start.isValid() || !end.isValid())
    {
        return "[<invalid>)";
    }
    return "[" + start.toString() + ", " + end.toString() + ")";
}

SequenceRange::SequenceRange() : start(FracturedNumber()), end(FracturedNumber())
{
}

std::ostream& operator<<(std::ostream& os, const SequenceRange& range)
{
    return os << range.toString();
}

}
