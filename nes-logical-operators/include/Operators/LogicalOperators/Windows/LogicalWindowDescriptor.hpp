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

#pragma once

#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Windowing
{

/// @brief The logical window definition encapsulates all information, which are required for windowed aggregations on data streams.
/// It contains the key attributes, the aggregation functions, and the window type.
class LogicalWindowDescriptor
{
public:
    explicit LogicalWindowDescriptor(
        const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& keys,
        std::vector<std::shared_ptr<WindowAggregationDescriptor>> windowAggregation,
        std::shared_ptr<WindowType> windowType);

    static std::shared_ptr<LogicalWindowDescriptor> create(
        const std::vector<std::shared_ptr<WindowAggregationDescriptor>>& windowAggregations, const std::shared_ptr<WindowType>& windowType);

    static std::shared_ptr<LogicalWindowDescriptor> create(
        const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& keys,
        std::vector<std::shared_ptr<WindowAggregationDescriptor>> windowAggregation,
        const std::shared_ptr<WindowType>& windowType);

    bool isKeyed() const;

    [[nodiscard]] uint64_t getNumberOfInputEdges() const;

    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    std::vector<std::shared_ptr<WindowAggregationDescriptor>> getWindowAggregation() const;

    void setWindowAggregation(const std::vector<std::shared_ptr<WindowAggregationDescriptor>>& windowAggregation);

    std::shared_ptr<WindowType> getWindowType() const;
    void setWindowType(std::shared_ptr<WindowType> windowType);

    std::vector<std::shared_ptr<FieldAccessLogicalFunction>> getKeys() const;
    void setOnKey(const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& keys);

    [[nodiscard]] OriginId getOriginId() const;

    void setOriginId(OriginId originId);

    std::shared_ptr<LogicalWindowDescriptor> clone() const;

    std::string toString() const;

    bool equal(std::shared_ptr<LogicalWindowDescriptor> otherWindowDefinition) const;
    const std::vector<OriginId>& getInputOriginIds() const;
    void setInputOriginIds(const std::vector<OriginId>& inputOriginIds);

private:
    std::vector<std::shared_ptr<WindowAggregationDescriptor>> windowAggregation;
    std::shared_ptr<WindowType> windowType;
    std::vector<std::shared_ptr<FieldAccessLogicalFunction>> onKey;
    uint64_t numberOfInputEdges = 0;
    std::vector<OriginId> inputOriginIds;
    OriginId originId = INVALID_ORIGIN_ID;
};
using LogicalWindowDescriptorPtr = std::shared_ptr<LogicalWindowDescriptor>;
}
