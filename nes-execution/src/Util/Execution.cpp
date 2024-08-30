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

#include <tuple>

#include <API/AttributeField.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Execution.hpp>
#include <nautilus/std/string.h>
#include <magic_enum.hpp>


namespace NES::QueryCompilation::Util
{
std::tuple<uint64_t, uint64_t, Runtime::Execution::Operators::TimeFunctionPtr>
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
            const auto& timeStampFieldName = windowType.getTimeCharacteristic()->getField()->getName();
            auto timeStampFieldRecord = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeStampFieldName);
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
                timeStampFieldRecord, windowType.getTimeCharacteristic()->getTimeUnit());
            return std::make_tuple(windowSize, windowSlide, std::move(timeFunction));
        }
    }
}

void logProxy(const char* message, const LogLevel logLevel)
{
    /// Calling the logger with the message and the log level
    switch (logLevel)
    {
        case LogLevel::LOG_NONE:
            break;
        case LogLevel::LOG_FATAL_ERROR:
            NES_FATAL_ERROR("{}", message);
            break;
        case LogLevel::LOG_ERROR:
            NES_ERROR("{}", message);
            break;
        case LogLevel::LOG_WARNING:
            NES_WARNING("{}", message);
            break;
        case LogLevel::LOG_INFO:
            NES_INFO("{}", message);
            break;
        case LogLevel::LOG_DEBUG:
            NES_DEBUG("{}", message);
            break;
        case LogLevel::LOG_TRACE:
            NES_TRACE("{}", message);
            break;
    }

}

} /// namespace NES::QueryCompilation::Util
