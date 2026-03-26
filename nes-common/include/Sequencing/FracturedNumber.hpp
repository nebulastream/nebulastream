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

#include <compare>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <vector>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

/// A hierarchical number type supporting recursive fracturing.
/// Examples: {1} = "1", {1,0,1} = "1.0.1"
/// Comparison is lexicographic on the components vector.
/// FracturedNumbers of different depths should only be compared within a SequenceRange context.
class FracturedNumber
{
public:
    /// Creates an invalid (empty) FracturedNumber
    FracturedNumber();

    /// Creates a depth-1 FracturedNumber: "n"
    explicit FracturedNumber(uint64_t n);

    /// Creates a FracturedNumber from explicit components
    explicit FracturedNumber(std::vector<uint64_t> components);

    /// Returns the depth (number of components)
    [[nodiscard]] size_t depth() const;

    /// Returns true if this is a valid (non-empty) FracturedNumber
    [[nodiscard]] bool isValid() const;

    /// Access a specific component level
    [[nodiscard]] uint64_t operator[](size_t level) const;

    /// Raw pointer to components data (for Nautilus pointer passing)
    [[nodiscard]] uint64_t* data();
    [[nodiscard]] const uint64_t* data() const;

    /// Returns the components vector
    [[nodiscard]] const std::vector<uint64_t>& getComponents() const;

    /// Lexicographic comparison — asserts that both have the same depth
    std::strong_ordering operator<=>(const FracturedNumber& other) const;
    bool operator==(const FracturedNumber& other) const;

    /// Returns a new FracturedNumber with an additional sub-level appended
    /// e.g., FracturedNumber(1).withSubLevel(3) = FracturedNumber({1, 3})
    [[nodiscard]] FracturedNumber withSubLevel(uint64_t sub) const;

    /// Returns a new FracturedNumber with the last component incremented
    /// e.g., FracturedNumber({1, 0}).incremented() = FracturedNumber({1, 1})
    [[nodiscard]] FracturedNumber incremented() const;

    [[nodiscard]] std::string toString() const;
    friend std::ostream& operator<<(std::ostream& os, const FracturedNumber& fn);

private:
    std::vector<uint64_t> components;
};

}

/// std::hash specialization
template <>
struct std::hash<NES::FracturedNumber>
{
    size_t operator()(const NES::FracturedNumber& fn) const noexcept;
};

FMT_OSTREAM(NES::FracturedNumber);
