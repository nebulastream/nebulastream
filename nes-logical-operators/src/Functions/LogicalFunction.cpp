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
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

NullLogicalFunction::NullLogicalFunction()
{
}

std::string NullLogicalFunction::explain(ExplainVerbosity) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return "";
}

std::shared_ptr<DataType> NullLogicalFunction::getDataType() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return nullptr;
}

LogicalFunction NullLogicalFunction::withDataType(std::shared_ptr<DataType>) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return *this;
}

LogicalFunction NullLogicalFunction::withInferredDataType(const Schema&) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return *this;
}

std::vector<LogicalFunction> NullLogicalFunction::getChildren() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return {};
}

LogicalFunction NullLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return *this;
}

std::string_view NullLogicalFunction::getType() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return "";
}

SerializableFunction NullLogicalFunction::serialize() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    return SerializableFunction();
}

bool NullLogicalFunction::operator==(const NullLogicalFunction&) const
{
    return false;
}

bool NullLogicalFunction::operator==(const LogicalFunctionConcept&) const
{
    return false;
}

LogicalFunction& LogicalFunction::operator=(const LogicalFunction& other)
{
    if (this != &other)
    {
        self = other.self->clone();
    }
    return *this;
}

bool LogicalFunction::operator==(const LogicalFunction& other) const
{
    return self->equals(*other.self);
}

LogicalFunction::LogicalFunction() : self(std::make_unique<Model<NullLogicalFunction>>(NullLogicalFunction{}))
{
}

LogicalFunction::LogicalFunction(const LogicalFunction& other) : self(other.self->clone())
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

std::shared_ptr<DataType> LogicalFunction::getDataType() const
{
    auto s = self->getDataType();
    PRECONDITION(s != nullptr, "Invariant violation: dataType is null in LogicalFunction::getDataType()");
    return s;
}

LogicalFunction LogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    return self->withDataType(dataType);
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
