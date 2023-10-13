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
#include <API/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/TimeCharacteristic.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>

namespace NES::Windowing {

WindowType::WindowType() = default;

bool WindowType::isTimeBasedWindowType() { return false; }

bool WindowType::isContentBasedWindowType() { return false; }

TimeBasedWindowTypePtr WindowType::asTimeBasedWindowType(std::shared_ptr<WindowType> windowType) {
    if (auto timeBasedWindowType = std::dynamic_pointer_cast<TimeBasedWindowType>(windowType)) {
        return timeBasedWindowType;
    } else {
        NES_ERROR("Can not cast the window type to a time based window type");
        NES_THROW_RUNTIME_ERROR("Can not cast the window type to a time based window type");
    }
}

ContentBasedWindowTypePtr WindowType::asContentBasedWindowType(std::shared_ptr<WindowType> windowType) {
    if (auto contentBasedWindowType = std::dynamic_pointer_cast<ContentBasedWindowType>(windowType)) {
        return contentBasedWindowType;
    } else {
        NES_ERROR("Can not cast the window type to a content based window type");
        NES_THROW_RUNTIME_ERROR("Can not cast the window type to a content based window type");
    }
}
}// namespace NES::Windowing
