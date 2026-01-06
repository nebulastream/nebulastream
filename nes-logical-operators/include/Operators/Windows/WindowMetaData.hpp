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

#include <ostream>
#include <utility>

#include "DataTypes/SchemaBase.hpp"
#include "DataTypes/UnboundField.hpp"
#include "Identifiers/Identifier.hpp"

namespace NES
{
struct WindowMetaData
{
    /// I'd prefer to use Fields or at least UnboundFields, but common cannot have a dependency on them
    /// and nes-phyiscal-operators depend on a lot of transitive dependencies at the moment.
    UnboundFieldBase<1> startField;
    UnboundFieldBase<1> endField;

    WindowMetaData(UnboundFieldBase<1> startField, UnboundFieldBase<1> endField);
    bool operator==(const WindowMetaData& other) const;
    friend std::ostream& operator<<(std::ostream& os, const WindowMetaData& obj);
};
}

FMT_OSTREAM(NES::WindowMetaData);

template <>
struct std::hash<NES::WindowMetaData>
{
    size_t operator()(const NES::WindowMetaData& windowMetaData) const noexcept;
};