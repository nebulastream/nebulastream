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

#include <cstddef>
#include <ostream>
#include <string>
#include <vector>
#include <Sequencing/FracturedNumber.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// A half-open range [start, end) of FracturedNumbers.
/// Supports merging adjacent ranges and fracturing a range into sub-ranges.
struct SequenceRange
{
    FracturedNumber start;
    FracturedNumber end;

    /// Constructs the initial range with a specific depth
    static SequenceRange initial(size_t depth);
    static SequenceRange invalid();

    /// Constructs a range [start, end). Both must be valid and start < end.
    SequenceRange(FracturedNumber start, FracturedNumber end);

    /// Returns true if both start and end are valid and start < end
    [[nodiscard]] bool isValid() const;

    /// Returns true if this->end == other.start (ranges are adjacent)
    [[nodiscard]] bool isAdjacentTo(const SequenceRange& other) const;

    /// Merge two adjacent ranges: [a, b) + [b, c) = [a, c)
    [[nodiscard]] SequenceRange merge(const SequenceRange& other) const;

    /// Fracture [a, b) into n sub-ranges by adding a sublevel.
    /// e.g., [1, 2).fracture(2) = {[1.0, 1.1), [1.1, 2.0)}
    [[nodiscard]] std::vector<SequenceRange> fracture(size_t n) const;

    /// Returns true if start <= point < end
    [[nodiscard]] bool contains(const FracturedNumber& point) const;
    [[nodiscard]] bool overlap(const SequenceRange& other) const;

    /// Returns true if this range fully covers the other range
    [[nodiscard]] bool covers(const SequenceRange& other) const;

    /// Comparison: assumes non-overlapping ranges. Compares by start.
    /// The no overlap assumption makes this a strong order
    std::strong_ordering operator<=>(const SequenceRange& other) const;
    bool operator==(const SequenceRange& other) const;

    [[nodiscard]] std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const SequenceRange& range);

private:
    SequenceRange();
};

}

FMT_OSTREAM(NES::SequenceRange);
