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

#include <memory>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Operators/UnaryLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <Traits/OriginIdTrait.hpp>

namespace NES
{

class WindowOperator : public UnaryLogicalOperator
{
public:
    WindowOperator(OriginId originId);
    WindowOperator();
    std::string_view getName() const noexcept override;

    [[nodiscard]] bool isKeyed() const;

    [[nodiscard]] uint64_t getNumberOfInputEdges() const;
    void setNumberOfInputEdges(uint64_t numberOfInputEdges);

    [[nodiscard]] std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> getWindowAggregation() const;
    void setWindowAggregation(const std::vector<std::shared_ptr<WindowAggregationLogicalFunction>>& windowAggregation);

    [[nodiscard]]std::shared_ptr<Windowing::WindowType> getWindowType() const;
    void setWindowType(std::shared_ptr<Windowing::WindowType> windowType);

    [[nodiscard]]std::vector<std::shared_ptr<FieldAccessLogicalFunction>> getKeys() const;
    void setOnKey(const std::vector<std::shared_ptr<FieldAccessLogicalFunction>>& keys);

    [[nodiscard]] OriginId getOriginId() const;
    const std::vector<OriginId>& getInputOriginIds() const;
    void setInputOriginIds(const std::vector<OriginId>& inputOriginIds);

    Optimizer::OriginIdTrait& get() {
        return originIds;
    }

protected:
    static constexpr std::string_view NAME = "Window";
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation;
    std::shared_ptr<Windowing::WindowType> windowType;
    std::vector<std::shared_ptr<FieldAccessLogicalFunction>> onKey;
    uint64_t numberOfInputEdges = 0;
    std::vector<OriginId> inputOriginIds;
    Optimizer::OriginIdTrait originIds;
};

}
