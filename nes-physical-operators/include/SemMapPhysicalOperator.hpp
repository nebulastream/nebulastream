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

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <Interface/Record.hpp>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <CompilationContext.hpp>
#include <LlmClient.hpp>
#include <PhysicalOperator.hpp>

namespace NES::detail
{
struct ThreadLocalLlmClients;
}

namespace NES
{

/// @brief Physical operator that evaluates a SEM_MAP model over one VARSIZED input field per
/// record via a blocking LLM round trip (plan §M1, "core risk").
class SemMapPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    /// `modelOutputNames` are the catalog OUTPUT names (what the prompt asks the LLM for and what
    /// keys `SemanticMapResult`); `outputFieldNames` are the record fields to write — the two are
    /// distinct when a call-site alias renames the model output (plan §M4). Both parallel, in
    /// declared order.
    SemMapPhysicalOperator(
        LlmClientFactory clientFactory,
        std::vector<QualifiedIdentifier> inputFieldNames,
        std::vector<QualifiedIdentifier> outputFieldNames,
        std::vector<std::string> modelOutputNames);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    /// shared_ptr is mandatory, not incidental: PhysicalOperator's type erasure copies the
    /// concrete operator by value, and nautilus::invoke bakes the pointer in as a trace-time
    /// constant — so the state's address must be stable.
    std::shared_ptr<detail::ThreadLocalLlmClients> threadLocal;
    std::vector<QualifiedIdentifier> inputFieldNames;
    std::vector<QualifiedIdentifier> outputFieldNames;
    std::optional<PhysicalOperator> child;
};

}
