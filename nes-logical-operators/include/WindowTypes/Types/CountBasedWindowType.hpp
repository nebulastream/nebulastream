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

#include <DataTypes/Schema.hpp>
#include <WindowTypes/Measures/CountMeasure.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Windowing
{

class CountBasedWindowType : public WindowType
{
public:
    explicit CountBasedWindowType();

    ~CountBasedWindowType() override = default;

    [[nodiscard]] virtual CountMeasure getSize() const = 0;

    [[nodiscard]] virtual CountMeasure getSlide() const = 0;

    bool inferStamp(const Schema& schema) override;
};

}