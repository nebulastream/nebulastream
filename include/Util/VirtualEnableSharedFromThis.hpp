/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_UTIL_VIRTUALENABLESHAREDFROMTHIS_HPP_
#define NES_INCLUDE_UTIL_VIRTUALENABLESHAREDFROMTHIS_HPP_

#include <memory>

namespace NES {
namespace detail {
/// base class for enabling enable_shared_from_this in classes with multiple super-classes that inherit enable_shared_from_this
struct virtual_enable_shared_from_this_base : std::enable_shared_from_this<virtual_enable_shared_from_this_base> {
    virtual ~virtual_enable_shared_from_this_base() {}
};

/// concrete class for enabling enable_shared_from_this in classes with multiple super-classes that inherit enable_shared_from_this
template<typename T>
struct virtual_enable_shared_from_this : virtual virtual_enable_shared_from_this_base {
    std::shared_ptr<T> shared_from_this() {
        return std::dynamic_pointer_cast<T>(virtual_enable_shared_from_this_base::shared_from_this());
    }
};
}
}// namespace NES

#endif//NES_INCLUDE_UTIL_VIRTUALENABLESHAREDFROMTHIS_HPP_
