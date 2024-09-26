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

#include <utility>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <magic_enum.hpp>

namespace NES
{

PhysicalSourceType::PhysicalSourceType(std::string logicalSourceName, SourceType sourceType)
    : logicalSourceName(std::move(logicalSourceName)), sourceType(sourceType)
{
}

const std::string& PhysicalSourceType::getLogicalSourceName() const
{
    return logicalSourceName;
}

SourceType PhysicalSourceType::getSourceType()
{
    return sourceType;
}

std::string PhysicalSourceType::getSourceTypeAsString()
{
    return std::string(magic_enum::enum_name(getSourceType()));
}
} /// namespace NES
