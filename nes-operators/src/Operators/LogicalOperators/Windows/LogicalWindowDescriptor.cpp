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
#include <utility>
#include <vector>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Windowing
{

LogicalWindowDescriptor::LogicalWindowDescriptor(
    const std::vector<NodeFunctionFieldAccessPtr>& keys,
    std::vector<WindowAggregationDescriptorPtr> windowAggregation,
    WindowTypePtr windowType)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(keys)
{
    NES_TRACE("LogicalWindowDescriptor: create new window definition");
}

bool LogicalWindowDescriptor::isKeyed() const
{
    return !onKey.empty();
}

LogicalWindowDescriptorPtr
LogicalWindowDescriptor::create(const std::vector<WindowAggregationDescriptorPtr>& windowAggregations, const WindowTypePtr& windowType)
{
    return create({}, windowAggregations, windowType);
}

LogicalWindowDescriptorPtr LogicalWindowDescriptor::create(
    const std::vector<NodeFunctionFieldAccessPtr>& keys,
    std::vector<WindowAggregationDescriptorPtr> windowAggregation,
    const WindowTypePtr& windowType)
{
    return std::make_shared<LogicalWindowDescriptor>(keys, windowAggregation, windowType);
}

uint64_t LogicalWindowDescriptor::getNumberOfInputEdges() const
{
    return numberOfInputEdges;
}
void LogicalWindowDescriptor::setNumberOfInputEdges(const uint64_t numberOfInputEdges)
{
    this->numberOfInputEdges = numberOfInputEdges;
}
std::vector<WindowAggregationDescriptorPtr> LogicalWindowDescriptor::getWindowAggregation() const
{
    return windowAggregation;
}

WindowTypePtr LogicalWindowDescriptor::getWindowType() const
{
    return windowType;
}

std::vector<NodeFunctionFieldAccessPtr> LogicalWindowDescriptor::getKeys() const
{
    return onKey;
}

void LogicalWindowDescriptor::setWindowAggregation(const std::vector<WindowAggregationDescriptorPtr>& windowAggregation)
{
    this->windowAggregation = windowAggregation;
}

void LogicalWindowDescriptor::setWindowType(WindowTypePtr windowType)
{
    this->windowType = windowType;
}

void LogicalWindowDescriptor::setOnKey(const std::vector<NodeFunctionFieldAccessPtr>& onKey)
{
    this->onKey = onKey;
}

LogicalWindowDescriptorPtr LogicalWindowDescriptor::copy() const
{
    return create(onKey, windowAggregation, windowType);
}

std::string LogicalWindowDescriptor::toString() const
{
    std::stringstream ss;
    ss << std::endl;
    ss << "windowType=" << windowType->toString();
    if (isKeyed())
    {
        ///ss << " onKey=" << onKey << std::endl;
    }
    ss << " numberOfInputEdges=" << numberOfInputEdges;
    ss << std::endl;
    return ss.str();
}
OriginId LogicalWindowDescriptor::getOriginId() const
{
    return originId;
}
void LogicalWindowDescriptor::setOriginId(const OriginId originId)
{
    this->originId = originId;
}

bool LogicalWindowDescriptor::equal(LogicalWindowDescriptorPtr otherWindowDefinition) const
{
    if (this->isKeyed() != otherWindowDefinition->isKeyed())
    {
        return false;
    }

    if (this->getKeys().size() != otherWindowDefinition->getKeys().size())
    {
        return false;
    }

    for (uint64_t i = 0; i < this->getKeys().size(); i++)
    {
        if (!this->getKeys()[i]->equal(otherWindowDefinition->getKeys()[i]))
        {
            return false;
        }
    }

    if (this->getWindowAggregation().size() != otherWindowDefinition->getWindowAggregation().size())
    {
        return false;
    }

    for (uint64_t i = 0; i < this->getWindowAggregation().size(); i++)
    {
        if (!this->getWindowAggregation()[i]->equal(otherWindowDefinition->getWindowAggregation()[i]))
        {
            return false;
        }
    }

    return this->windowType->equal(otherWindowDefinition->getWindowType());
}
const std::vector<OriginId>& LogicalWindowDescriptor::getInputOriginIds() const
{
    return inputOriginIds;
}
void LogicalWindowDescriptor::setInputOriginIds(const std::vector<OriginId>& inputOriginIds)
{
    LogicalWindowDescriptor::inputOriginIds = inputOriginIds;
}

}
