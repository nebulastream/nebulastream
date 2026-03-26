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

#include <Sequencing/FracturedNumber.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <ErrorHandling.hpp>

namespace NES
{

FracturedNumber::FracturedNumber() = default;

FracturedNumber::FracturedNumber(uint64_t n) : components{n} { }

FracturedNumber::FracturedNumber(std::vector<uint64_t> components) : components(std::move(components)) { }

size_t FracturedNumber::depth() const
{
    return components.size();
}

bool FracturedNumber::isValid() const
{
    return !components.empty();
}

uint64_t FracturedNumber::operator[](size_t level) const
{
    PRECONDITION(level < components.size(), "Level {} out of range for FracturedNumber with depth {}", level, components.size());
    return components[level];
}

uint64_t* FracturedNumber::data()
{
    return components.data();
}

const uint64_t* FracturedNumber::data() const
{
    return components.data();
}

const std::vector<uint64_t>& FracturedNumber::getComponents() const
{
    return components;
}

std::strong_ordering FracturedNumber::operator<=>(const FracturedNumber& other) const
{
    PRECONDITION(
        components.size() == other.components.size(),
        "Cannot compare FracturedNumbers of different depths: {} vs {}",
        toString(),
        other.toString());
    return components <=> other.components;
}

bool FracturedNumber::operator==(const FracturedNumber& other) const
{
    PRECONDITION(
        components.size() == other.components.size(),
        "Cannot compare FracturedNumbers of different depths: {} vs {}",
        toString(),
        other.toString());
    return components == other.components;
}

FracturedNumber FracturedNumber::withSubLevel(uint64_t sub) const
{
    PRECONDITION(isValid(), "Cannot add sub-level to invalid FracturedNumber");
    auto newComponents = components;
    newComponents.push_back(sub);
    return FracturedNumber(std::move(newComponents));
}

FracturedNumber FracturedNumber::incremented() const
{
    PRECONDITION(isValid(), "Cannot increment invalid FracturedNumber");
    auto newComponents = components;
    newComponents.back() += 1;
    return FracturedNumber(std::move(newComponents));
}

std::string FracturedNumber::toString() const
{
    if (components.empty())
    {
        return "<invalid>";
    }
    std::ostringstream oss;
    for (size_t i = 0; i < components.size(); ++i)
    {
        if (i > 0)
        {
            oss << '.';
        }
        oss << components[i];
    }
    return oss.str();
}

std::ostream& operator<<(std::ostream& os, const FracturedNumber& fn)
{
    return os << fn.toString();
}

}

size_t std::hash<NES::FracturedNumber>::operator()(const NES::FracturedNumber& fn) const noexcept
{
    size_t seed = 0;
    for (const auto& c : fn.getComponents())
    {
        seed ^= std::hash<uint64_t>{}(c) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
}
