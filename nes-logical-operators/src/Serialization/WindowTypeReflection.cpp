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

#include <Serialization/WindowTypeReflection.hpp>

#include <memory>
#include <utility>

#include <Util/Reflection.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

Reflected reflectWindowType(const Windowing::WindowType& windowType)
{
    auto [type, reflectedWindow] = [&windowType]()
    {
        if (typeid(windowType) == typeid(Windowing::TumblingWindow))
        {
            const auto tumblingWindow = dynamic_cast<const Windowing::TumblingWindow&>(windowType);
            return std::make_pair("TumblingWindow", reflect(tumblingWindow));
        }
        if (typeid(windowType) == typeid(Windowing::SlidingWindow))
        {
            const auto slidingWindow = dynamic_cast<const Windowing::SlidingWindow&>(windowType);
            return std::make_pair("SlidingWindow", reflect(slidingWindow));
        }
        throw CannotSerialize("Cannot serialize unknown window type");
    }();

    return reflect(detail::ReflectedWindowTypeReflection{.type = type, .config = reflectedWindow});
}

std::shared_ptr<Windowing::WindowType> unreflectWindowType(const Reflected& reflected)
{
    auto [type, config] = unreflect<detail::ReflectedWindowTypeReflection>(reflected);

    auto windowType = [&] -> std::shared_ptr<Windowing::WindowType>
    {
        if (type == "TumblingWindow")
        {
            return std::make_shared<Windowing::TumblingWindow>(unreflect<Windowing::TumblingWindow>(config));
        }
        if (type == "SlidingWindow")
        {
            return std::make_shared<Windowing::SlidingWindow>(unreflect<Windowing::SlidingWindow>(config));
        }
        throw CannotDeserialize("Cannot deserialize unknown window type {}", type);
    }();

    return windowType;
}


}
