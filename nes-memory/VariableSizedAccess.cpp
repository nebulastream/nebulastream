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
#include <Runtime/VariableSizedAccess.hpp>

#include <cstdint>
#include <ostream>
#include <ErrorHandling.hpp>

namespace NES
{
VariableSizedAccess::Index::Index(const uint64_t index) : index(index)
{
}

VariableSizedAccess::Index::Underlying VariableSizedAccess::Index::getRawIndex() const
{
    return index;
}

std::ostream& operator<<(std::ostream& os, const VariableSizedAccess::Index& index)
{
    return os << index.index;
}

VariableSizedAccess::Index::Underlying
operator/(const VariableSizedAccess::Index& index, const VariableSizedAccess::Index::Underlying other)
{
    return index.index / other;
}

VariableSizedAccess::Index::Underlying
operator%(const VariableSizedAccess::Index& index, const VariableSizedAccess::Index::Underlying other)
{
    return index.index % other;
}

VariableSizedAccess::Offset::Offset(const uint64_t offset) : offset(offset)
{
}

VariableSizedAccess::Offset::Underlying VariableSizedAccess::Offset::getRawOffset() const
{
    return offset;
}

std::ostream& operator<<(std::ostream& os, const VariableSizedAccess::Offset& offset)
{
    return os << offset.offset;
}

VariableSizedAccess::Size::Size(uint64_t size) : size(size)
{
}

VariableSizedAccess::Size::Underlying VariableSizedAccess::Size::getRawSize() const
{
    return size;
}


}
