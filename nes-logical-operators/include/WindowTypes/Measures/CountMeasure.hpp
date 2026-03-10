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
#include <string>
#include <Util/Reflection.hpp>

namespace NES::Windowing
{
/// A count based window measure.
class CountMeasure
{
public:
    explicit CountMeasure(uint64_t count);

    [[nodiscard]] uint64_t getCount() const;

    [[nodiscard]] std::string toString() const;

    bool operator<(const CountMeasure& other) const;
    bool operator<=(const CountMeasure& other) const;
    bool operator==(const CountMeasure& other) const;

private:
    const uint64_t count;
};

}

namespace NES
{
template <>
struct Reflector<Windowing::CountMeasure>
{
    Reflected operator()(const Windowing::CountMeasure& measure) const;
};

template <>
struct Unreflector<Windowing::CountMeasure>
{
    Windowing::CountMeasure operator()(const Reflected& reflected) const;
};
}