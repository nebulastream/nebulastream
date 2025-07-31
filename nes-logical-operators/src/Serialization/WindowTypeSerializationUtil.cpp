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
#include <Serialization/WindowTypeSerializationUtil.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include "ErrorHandling.hpp"

namespace NES
{
SerializableWindowType* WindowTypeSerializationUtil::serializeWindowType(
    const std::shared_ptr<Windowing::WindowType>& windowType, SerializableWindowType* serializableWindowType)
{
    if (auto timeBasedWindow = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType))
    {
        if (auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType))
        {
            auto* tumbling = serializableWindowType->mutable_tumbling_window();
            tumbling->set_size(tumblingWindow->getSize().getTime());
        }
        else if (auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType))
        {
            auto* sliding = serializableWindowType->mutable_sliding_window();
            sliding->set_size(slidingWindow->getSize().getTime());
            sliding->set_slide(slidingWindow->getSlide().getTime());
        }
    }
    return serializableWindowType;
}

std::shared_ptr<Windowing::WindowType>
WindowTypeSerializationUtil::deserializeWindowType(const SerializableWindowType& serializedWindowType)
{
    if (serializedWindowType.has_tumbling_window())
    {
        return std::make_shared<Windowing::TumblingWindow>(Windowing::TimeMeasure(serializedWindowType.tumbling_window().size()));
    }
    if (serializedWindowType.has_sliding_window())
    {
        return Windowing::SlidingWindow::of(
            Windowing::TimeMeasure(serializedWindowType.sliding_window().size()),
            Windowing::TimeMeasure(serializedWindowType.sliding_window().slide()));
    }
    throw CannotDeserialize("Cannot deserialize unknown window type");
}
}
