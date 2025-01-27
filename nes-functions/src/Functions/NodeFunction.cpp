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
#include <Functions/NodeFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
NodeFunction::NodeFunction(std::shared_ptr<DataType> stamp, std::string type) : stamp(std::move(stamp)), type(std::move(type))
{
}

bool NodeFunction::isPredicate() const
{
    return NES::Util::instanceOf<Boolean>(stamp);
}

std::shared_ptr<DataType> NodeFunction::getStamp() const
{
    return stamp;
}

void NodeFunction::setStamp(std::shared_ptr<DataType> stamp)
{
    this->stamp = std::move(stamp);
}

const std::string& NodeFunction::getType() const
{
    return type;
}

void NodeFunction::inferStamp(const Schema& schema)
{
    /// infer stamp on all children nodes
    for (const auto& node : children)
    {
        NES::Util::as<NodeFunction>(node)->inferStamp(schema);
    }
}

NodeFunction::NodeFunction(const NodeFunction* other) : stamp(other->stamp)
{
}

}
