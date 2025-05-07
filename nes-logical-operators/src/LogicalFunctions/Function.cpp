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
#include <LogicalFunctions/Function.hpp>
#include <Util/Common.hpp>
#include <SerializableFunction.pb.h>

namespace NES::Logical
{

NullFunction::NullFunction()
{
}

std::string NullFunction::explain(ExplainVerbosity) const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

std::shared_ptr<DataType> NullFunction::getStamp() const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

Function NullFunction::withStamp(std::shared_ptr<DataType>) const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

Function NullFunction::withInferredStamp(const Schema&) const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

std::vector<Function> NullFunction::getChildren() const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

Function NullFunction::withChildren(const std::vector<Function>&) const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

std::string_view NullFunction::getType() const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

SerializableFunction NullFunction::serialize() const
{
    PRECONDITION(false, "Calls in NullFunction are undefined");
}

bool NullFunction::operator==(const NullFunction&) const
{
    return false;
}

bool NullFunction::operator==(const FunctionConcept&) const
{
    return false;
}

Function& Function::operator=(const Function& other)
{
    if (this != &other)
    {
        self = other.self->clone();
    }
    return *this;
}

bool Function::operator==(const Function& other) const
{
    return self->equals(*other.self);
}

Function::Function() : self(std::make_unique<Model<NullFunction>>(NullFunction{}))
{
}

Function::Function(const Function& other) : self(other.self->clone())
{
}

std::string Function::explain(ExplainVerbosity verbosity) const
{
    return self->explain(verbosity);
}

Function Function::withInferredStamp(const Schema& schema) const
{
    return self->withInferredStamp(schema);
}

std::vector<Function> Function::getChildren() const
{
    return self->getChildren();
}

Function Function::withChildren(const std::vector<Function>& children) const
{
    return self->withChildren(children);
}

std::shared_ptr<DataType> Function::getStamp() const
{
    auto s = self->getStamp();
    PRECONDITION(s != nullptr, "Invariant violation: Stamp is null in Function::getStamp()");
    return s;
}

Function Function::withStamp(std::shared_ptr<DataType> stamp) const
{
    return self->withStamp(stamp);
}

SerializableFunction Function::serialize() const
{
    return self->serialize();
}

std::string_view Function::getType() const
{
    return self->getType();
}

}