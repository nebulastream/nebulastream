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

#include <cstdint>
#include <memory>
#include <tuple>

#include <API/AttributeField.hpp>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Execution.hpp>
#include <nautilus/std/string.h>

namespace NES::QueryCompilation::Util
{
std::tuple<uint64_t, uint64_t, std::unique_ptr<Runtime::Execution::Operators::TimeFunction>>
getWindowingParameters(Windowing::TimeBasedWindowType& windowType)
{
    const auto& windowSize = windowType.getSize().getTime();
    const auto& windowSlide = windowType.getSlide().getTime();
    const auto type = windowType.getTimeCharacteristic()->getType();

    switch (type)
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
            return std::make_tuple(windowSize, windowSlide, std::move(timeFunction));
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            const auto& timeStampFieldName = windowType.getTimeCharacteristic()->field->getName();
            auto timeStampFieldRecord = std::make_unique<Runtime::Execution::Functions::ExecutableFunctionReadField>(timeStampFieldName);
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
                std::move(timeStampFieldRecord), windowType.getTimeCharacteristic()->getTimeUnit());
            return std::make_tuple(windowSize, windowSlide, std::move(timeFunction));
        }
    }
}
}
