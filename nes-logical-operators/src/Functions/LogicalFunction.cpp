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
#include <SerializableFunction.pb.h>

namespace NES
{

NullLogicalFunction::NullLogicalFunction()
{
}

std::string NullLogicalFunction::explain(ExplainVerbosity) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
}

std::shared_ptr<DataType> NullLogicalFunction::getStamp() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
}

LogicalFunction NullLogicalFunction::withStamp(std::shared_ptr<DataType>) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
}

LogicalFunction NullLogicalFunction::withInferredStamp(const Schema&) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
}

std::vector<LogicalFunction> NullLogicalFunction::getChildren() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
}

LogicalFunction NullLogicalFunction::withChildren(const std::vector<LogicalFunction>&) const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
}

std::string_view NullLogicalFunction::getType() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
}

SerializableFunction NullLogicalFunction::serialize() const
{
    PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
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

LogicalFunction LogicalFunction::withInferredStamp(const Schema& schema) const
{
    return self->withInferredStamp(schema);
}

std::vector<LogicalFunction> LogicalFunction::getChildren() const
{
    return self->getChildren();
}

LogicalFunction LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    return self->withChildren(children);
}

std::shared_ptr<DataType> LogicalFunction::getStamp() const
{
    auto s = self->getStamp();
    PRECONDITION(s != nullptr, "Invariant violation: Stamp is null in LogicalFunction::getStamp()");
    return s;
}

LogicalFunction LogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    return self->withStamp(stamp);
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
