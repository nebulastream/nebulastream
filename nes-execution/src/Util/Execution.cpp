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

#include <API/AttributeField.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Execution.hpp>
#include <tuple>

namespace NES::QueryCompilation::Util {
    std::tuple<uint64_t, uint64_t, Runtime::Execution::Operators::TimeFunctionPtr>
        getWindowingParameters(Windowing::TimeBasedWindowType& windowType) {
        const auto& windowSize = windowType.getSize().getTime();
        const auto& windowSlide = windowType.getSlide().getTime();
        const auto type = windowType.getTimeCharacteristic()->getType();

        switch (type) {
            case Windowing::TimeCharacteristic::Type::IngestionTime: {
                auto timeFunction = std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
                return std::make_tuple(windowSize, windowSlide, std::move(timeFunction));
            }
            case Windowing::TimeCharacteristic::Type::EventTime: {
                const auto& timeStampFieldName = windowType.getTimeCharacteristic()->getField()->getName();
                auto timeStampFieldRecord =
                    std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeStampFieldName);
                auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(timeStampFieldRecord);
                return std::make_tuple(windowSize, windowSlide, std::move(timeFunction));
            }
        }
    }
} // namespace NES::QueryCompilation::Util