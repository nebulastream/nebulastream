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
#include <sstream>
#include <utility>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/ArithmeticalFunctions/ModuloBinaryLogicalFunction.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

ModuloBinaryLogicalFunction::ModuloBinaryLogicalFunction(std::shared_ptr<DataType> stamp)
    : BinaryLogicalFunction(std::move(stamp), "Mod") {};

ModuloBinaryLogicalFunction::ModuloBinaryLogicalFunction(ModuloBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
ModuloBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto addNode = std::make_shared<ModuloBinaryLogicalFunction>(DataTypeProvider::provideDataType(LogicalType::FLOAT32));
    addNode->setChildren(left, right);
    return addNode;
}

bool ModuloBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<ModuloBinaryLogicalFunction>(rhs))
    {
        auto otherAddNode = NES::Util::as<ModuloBinaryLogicalFunction>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string ModuloBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "%" << *children[1];
    return ss.str();
}

std::shared_ptr<LogicalFunction> ModuloBinaryLogicalFunction::deepCopy()
{
    return ModuloBinaryLogicalFunction::create(
        Util::as<LogicalFunction>(children[0])->deepCopy(), Util::as<LogicalFunction>(children[1])->deepCopy());
}

}
