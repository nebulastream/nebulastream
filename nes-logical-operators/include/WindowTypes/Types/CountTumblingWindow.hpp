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

#include <string>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/CountMeasure.hpp>
#include <WindowTypes/Types/CountBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Windowing
{

class CountTumblingWindow final : public CountBasedWindowType
{
public:
    explicit CountTumblingWindow(CountMeasure size);

    [[nodiscard]] CountMeasure getSize() const override;
    [[nodiscard]] CountMeasure getSlide() const override;
    [[nodiscard]] std::string toString() const override;
    bool operator==(const WindowType& otherWindowType) const override;

private:
    const CountMeasure size;
};

}

namespace NES
{
template <>
struct Reflector<Windowing::CountTumblingWindow>
{
    Reflected operator()(const Windowing::CountTumblingWindow& tumblingWindow) const;
};

template <>
struct Unreflector<Windowing::CountTumblingWindow>
{
    Windowing::CountTumblingWindow operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedCountTumblingWindow
{
    Windowing::CountMeasure size{0};
};
}