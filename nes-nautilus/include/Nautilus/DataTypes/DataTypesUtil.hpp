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
#include <Nautilus/Interface/NESStrongTypeRef.hpp>

#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Nautilus::Util
{

template <typename T>
nautilus::val<T> readValueFromMemRef(nautilus::val<int8_t*> memRef)
{
    return static_cast<nautilus::val<T>>(*static_cast<nautilus::val<T*>>(memRef));
}

}
