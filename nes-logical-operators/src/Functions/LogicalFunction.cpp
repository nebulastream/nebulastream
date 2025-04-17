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

#include <memory>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

LogicalFunction::LogicalFunction(std::shared_ptr<DataType> stamp, std::string type) : stamp(std::move(stamp)), type(type)
{
}

bool LogicalFunction::isPredicate() const
{
    return NES::Util::instanceOf<Boolean>(stamp);
}

std::shared_ptr<DataType> LogicalFunction::getStamp() const
{
    return stamp;
}

void LogicalFunction::setStamp(std::shared_ptr<DataType> stamp)
{
    this->stamp = std::move(stamp);
}

void LogicalFunction::inferStamp(const Schema& schema)
{
    for (const auto& node : children)
    {
        NES::Util::as<LogicalFunction>(node)->inferStamp(schema);
    }
}

LogicalFunction::LogicalFunction(const LogicalFunction& other) : stamp(other.stamp)
{
}

}
