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

#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{
ConstantValueVariableSizePhysicalFunction::ConstantValueVariableSizePhysicalFunction(const int8_t* value, const size_t size) : data(size)
{
    /// We copy the value into the data vector owned by the function
    /// since the value might be destroyed during the lifetime of the function.
    /// In the constructor, we have allocated the memory for the value via the std::vector ctor.
    std::memcpy(data.data(), value, size);
}

VarVal ConstantValueVariableSizePhysicalFunction::execute(const Record&, ArenaRef&) const
{
    /// Embed the literal's bytes as a private constant global in the JIT module instead of an
    /// opaque host pointer: downstream operations on the literal (a lazy string equality's
    /// memcmp, a copy into the output buffer) then have a compile-time-visible source and can
    /// fold. In interpreted mode this degrades to the host pointer into `data` (owned by this
    /// function, which outlives execution). VariableSizedData requires a non-const pointer but
    /// the constant is never written through.
    const auto embedded = nautilus::embedConstantBytes(data.data(), data.size());
    VariableSizedData result(static_cast<nautilus::val<int8_t*>>(embedded), data.size());
    return result;
}

}
