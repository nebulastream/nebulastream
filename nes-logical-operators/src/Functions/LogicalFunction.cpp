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

#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <typeinfo>
#ifdef __GNUG__
#include <cxxabi.h>
#include <cstdlib>
#endif

namespace NES
{

void LogicalFunction::setStamp(std::unique_ptr<DataType> stamp)
{
    this->stamp = std::move(stamp);
}

const DataType& LogicalFunction::getStamp() const
{
    return *stamp;
}

void LogicalFunction::inferStamp(const Schema& schema)
{
    for (const auto& node : getChildren())
    {
        node->inferStamp(schema);
    }
}

std::string LogicalFunction::getType() const {
#ifdef __GNUG__
    int status = 0;
    /// Demangle the name obtained from typeid.
    char* demangled = abi::__cxa_demangle(typeid(*this).name(), nullptr, nullptr, &status);
    std::string result = (status == 0) ? demangled : typeid(*this).name();
    std::free(demangled);
    return result;
#else
    /// For non-GNU compilers, typeid may already return a readable name.
    return typeid(*this).name();
#endif
}

LogicalFunction::LogicalFunction(const LogicalFunction& other) : stamp(other.stamp ? other.stamp->clone() : nullptr)
{
}

}
