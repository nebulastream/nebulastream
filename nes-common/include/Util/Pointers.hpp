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
#include <experimental/propagate_const>

namespace NES
{

template <typename T>
using SharedPtr = std::experimental::propagate_const<std::shared_ptr<T>>;

template <typename T>
using UniquePtr = std::experimental::propagate_const<std::unique_ptr<T>>;

template <typename T>
std::shared_ptr<const T> copyPtr(const SharedPtr<T>& ptr)
{
    return std::shared_ptr<const T>{std::experimental::get_underlying(ptr)};
}

template <typename T>
std::shared_ptr<T> copyPtr(SharedPtr<T>& ptr)
{
    return std::shared_ptr<T>{std::experimental::get_underlying(ptr)};
}

}
