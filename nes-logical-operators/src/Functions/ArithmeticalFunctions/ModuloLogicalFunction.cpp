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
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/BinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES
{

ModuloLogicalFunction::ModuloLogicalFunction(std::shared_ptr<DataType> stamp) : BinaryLogicalFunction(std::move(stamp), "Mod") {};

ModuloLogicalFunction::ModuloLogicalFunction(ModuloLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
ModuloLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto addNode = std::make_shared<ModuloLogicalFunction>(DataTypeProvider::provideDataType(LogicalType::FLOAT32));
    addNode->setLeftChild(left);
    addNode->setRightChild(right);
    return addNode;
}

bool ModuloLogicalFunction::operator==(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<ModuloLogicalFunction>(rhs))
    {
        auto otherAddNode = NES::Util::as<ModuloLogicalFunction>(rhs);
        return getLeftChild()->equal(otherAddNode->getLeftChild()) && getRightChild()->equal(otherAddNode->getRightChild());
    }
    return false;
}

std::string ModuloLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "%" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> ModuloLogicalFunction::clone() const
{
    return ModuloLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

}
