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

#include <Operators/Windows/WindowMetaData.hpp>
#include <folly/Hash.h>

NES::WindowMetaData::WindowMetaData(UnboundFieldBase<1> startField, UnboundFieldBase<1> endField) : startField(std::move(startField)), endField(std::move(endField))
{
}

bool NES::WindowMetaData::operator==(const WindowMetaData& other) const = default;

std::size_t std::hash<NES::WindowMetaData>::operator()(const NES::WindowMetaData& windowMetaData) const noexcept
{
    return folly::hash::hash_combine(windowMetaData.startField, windowMetaData.endField);
}

std::ostream& NES::operator<<(std::ostream& os, const WindowMetaData& obj)
{
    return os << fmt::format("startField: {}, endField {}", obj.startField, obj.endField);
}