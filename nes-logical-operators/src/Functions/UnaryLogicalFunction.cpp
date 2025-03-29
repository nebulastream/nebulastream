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

#include <string>
#include <memory>
#include <utility>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>

namespace NES
{
LogicalFunction::LogicalFunction(std::shared_ptr<DataType> stamp, std::string name)
    : LogicalFunction(std::move(stamp), std::move(name))
{
}

LogicalFunction::LogicalFunction(LogicalFunction* other) : LogicalFunction(other)
{
}

void LogicalFunction::setChild(const std::shared_ptr<LogicalFunction> child)
{
    children[0] = child;
}

std::shared_ptr<LogicalFunction> LogicalFunction::getChild() const
{
    return children[0];
}
}
