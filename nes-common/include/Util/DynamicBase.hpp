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

namespace NES
{
namespace detail
{
struct DynamicBase
{
    virtual ~DynamicBase() = default;
};
}

/// If your operator inherits from some type T,
/// you will also need to inherit from Castable<T> if you want to be able to cast with <tt>TypedLogicalOperator<Checked>::getAs<T>()</tt>.
/// If you control T, you can also use it as a CRTP and inherit it directly @code class T : Castable<T> @endcode
/// This class is only there to add a commonly accessible VTable so that we can cast the pointer to the operator.
/// @tparam T the type to be able to cast to from LogicalOperator
template <typename T>
struct Castable : NES::detail::DynamicBase
{
    const T& get() const { return *dynamic_cast<const T*>(this); }

    const T* operator->() const { return dynamic_cast<const T*>(this); }

    const T& operator*() const { return *dynamic_cast<const T*>(this); }

private:
    Castable() = default;
    friend T;
};
}
