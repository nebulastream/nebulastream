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
#include <utility>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>

namespace NES
{

/// The InlineSourceBindingPhase replaces all sources that are defined within the query itself (InlineSourceLogicalOperators), as opposed to
/// sources that are created in separate CREATE statements, with physical sources based on the given inline source configuration.

class InlineSourceBindingPhase
{
public:
    explicit InlineSourceBindingPhase(std::shared_ptr<const SourceCatalog> sourceCatalog) : sourceCatalog(std::move(sourceCatalog)) { }

    void apply(LogicalPlan& queryPlan) const;

private:
    [[nodiscard]] LogicalOperator bindInlineSourceLogicalOperators(const LogicalOperator& current) const;
    std::shared_ptr<const SourceCatalog> sourceCatalog;
};

}
