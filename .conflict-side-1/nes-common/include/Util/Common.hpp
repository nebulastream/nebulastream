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
#include <memory>
#include <nameof.hpp>

#include <ErrorHandling.hpp>

namespace NES
{
/// cast the given object to the specified type.
template <typename Out, typename In>
std::shared_ptr<Out> as(const std::shared_ptr<In>& obj)
{
    if (auto ptr = std::dynamic_pointer_cast<Out>(obj))
    {
        return ptr;
    }
    throw InvalidDynamicCast("Invalid dynamic cast: from {} to {}", NAMEOF_TYPE(In), NAMEOF_TYPE(Out));
}

/// cast the given object to the specified type.
template <typename Out, typename In>
Out as(const In& obj)
{
    return *dynamic_cast<Out*>(&obj);
}

}
