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
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

bool LogicalFunction::operator==(const LogicalFunction& other) const
{
    return self->equals(*other.self);
}

LogicalFunction::LogicalFunction() : self(nullptr)
{
}

std::string LogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return self->explain(verbosity);
}

LogicalFunction LogicalFunction::withInferredDataType(const Schema& schema) const
{
    return self->withInferredDataType(schema);
}

std::vector<LogicalFunction> LogicalFunction::getChildren() const
{
    return self->getChildren();
}

LogicalFunction LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    return self->withChildren(children);
}

DataType LogicalFunction::getDataType() const
{
    return self->getDataType();
}

LogicalFunction LogicalFunction::withDataType(const DataType& dataType) const
{
    return self->withDataType(std::move(dataType));
}

SerializableFunction LogicalFunction::serialize() const
{
    return self->serialize();
}

std::string_view LogicalFunction::getType() const
{
    return self->getType();
}

}
