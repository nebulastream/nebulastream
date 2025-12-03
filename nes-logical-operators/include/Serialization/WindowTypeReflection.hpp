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

#include <WindowTypes/Types/WindowType.hpp>

#include <memory>
#include <string>
#include <Util/Reflection.hpp>

namespace NES
{

Reflected reflectWindowType(std::shared_ptr<Windowing::WindowType> windowType);
std::shared_ptr<Windowing::WindowType> unreflectWindowType(const Reflected& reflected);

}

namespace NES::detail
{
struct ReflectedWindowTypeReflection
{
    std::string type;
    Reflected config;
};
}
