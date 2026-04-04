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

#include <any>
#include <cstdint>
#include <DataTypes/DataType.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES
{

class VarVal;

/// Stores a raw (unparsed) field value as a pointer + size into the input buffer.
/// Equality comparisons use size-check + memcmp, avoiding the cost of from_chars parsing.
/// Non-equality operators trigger materialization (deferred parsing) via the stored materializer.
struct RawValue
{
    nautilus::val<int8_t*> data;
    nautilus::val<uint64_t> size;
    DataType::Type type;
    std::any materializer; /// Type-erased std::function<VarVal()>; set by storeRawValueInRecord

    /// Raw equality: same type, size check + memcmp
    nautilus::val<bool> operator==(const RawValue& rhs) const;
    nautilus::val<bool> operator!=(const RawValue& rhs) const;

    /// Parse the raw bytes into a typed VarVal via the stored materializer
    VarVal materialize() const;
};

}
