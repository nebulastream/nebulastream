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
#include <Abstract/PhysicalOperator.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Map operator that evaluates a map function on a input records.
/// Map functions read record fields, apply transformations, and can set/update fields.
class MapPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    MapPhysicalOperator(Record::RecordFieldIdentifier fieldToWriteTo, Functions::PhysicalFunction mapFunction);
    void execute(ExecutionContext& ctx, Record& record) const override;

    std::optional<PhysicalOperator> getChild() const override { return child; }
    void setChild(struct PhysicalOperator child) override { this->child = child; }

private:
    Record::RecordFieldIdentifier fieldToWriteTo;
    Functions::PhysicalFunction mapFunction;
    static constexpr bool PIPELINE_BREAKER = false;
    std::optional<PhysicalOperator> child;
};

}
