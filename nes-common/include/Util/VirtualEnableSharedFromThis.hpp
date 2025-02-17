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

#include <cstddef>
#include <memory>

#if defined(__GLIBCXX__) || defined(__GLIBCPP__)
    #define NES_NOEXCEPT(isNoexcept) noexcept(isNoexcept)
#else
    #define NES_NOEXCEPT(isNoexcept)
#endif

namespace NES::detail
{
/// base class for enabling enable_shared_from_this in classes with multiple super-classes that inherit enable_shared_from_this
template <bool isNoexceptDestructible>
struct virtual_enable_shared_from_this_base : std::enable_shared_from_this<virtual_enable_shared_from_this_base<isNoexceptDestructible>>
{
    virtual ~virtual_enable_shared_from_this_base() NES_NOEXCEPT(isNoexceptDestructible) = default;
};

/// concrete class for enabling enable_shared_from_this in classes with multiple super-classes that inherit enable_shared_from_this
template <typename T, bool isNoexceptDestructible = true>
struct virtual_enable_shared_from_this : virtual virtual_enable_shared_from_this_base<isNoexceptDestructible>
{
    ~virtual_enable_shared_from_this() NES_NOEXCEPT(isNoexceptDestructible) override = default;

    ///TODO: fix Function 'shared_from_this' hides a non-virtual function from struct 'enable_shared_from_this<virtual_enable_shared_from_this_base<isNoexceptDestructible>>'
    template <typename T1 = T>
    std::shared_ptr<T1> shared_from_this()
    {
        return std::dynamic_pointer_cast<T1>(virtual_enable_shared_from_this_base<isNoexceptDestructible>::shared_from_this());
    }

    ///TODO: fix Function 'weak_from_this' hides a non-virtual function from struct 'enable_shared_from_this<virtual_enable_shared_from_this_base<isNoexceptDestructible>>'
    template <typename T1 = T>
    std::weak_ptr<T1> weak_from_this()
    {
        return std::dynamic_pointer_cast<T1>(virtual_enable_shared_from_this_base<isNoexceptDestructible>::weak_from_this().lock());
    }
};

}
