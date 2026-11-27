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

#include <cstdint>
#include <functional>
#include <utility>

#include <DataTypes/VarVal.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Variable-sized seam used only for the VARSIZED branch, since varsized payloads live outside the
/// fixed-size slot (in a PagedVector page or a TupleBuffer child buffer, depending on the layout).
/// Load resolves the (pointer, length) of the payload; store writes the payload at the slot.
using VarSizedLoadFn = std::function<std::pair<nautilus::val<int8_t*>, nautilus::val<uint64_t>>(nautilus::val<int8_t*> slot)>;
using VarSizedStoreFn = std::function<void(nautilus::val<int8_t*> slot, const VarVal& value)>;

}
