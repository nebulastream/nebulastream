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
namespace NES::Windowing
{
/// A time based window measure.
class TimeMeasure
{
public:
    explicit TimeMeasure(uint64_t milliseconds);

    [[nodiscard]] uint64_t getTime() const;

    [[nodiscard]] std::string toString() const;

    bool operator<(const TimeMeasure& other) const;
    bool operator<=(const TimeMeasure& other) const;
    bool operator==(const TimeMeasure& other) const;

private:
    const uint64_t milliSeconds;
};

}
