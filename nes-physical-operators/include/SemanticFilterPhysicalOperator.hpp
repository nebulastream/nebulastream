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
#include <optional>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <CompilationContext.hpp>
#include <PhysicalOperator.hpp>
#include <SemanticBackend.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES::detail
{
struct SemanticFilterState;
}

namespace NES
{

/// Evaluates SEM_FILTER record by record with one blocking LLM round trip each (stage 1), and hands
/// on only the records the answer affirms. The record passes through unchanged; a fused operator
/// whose step list also holds MAP steps writes one VARSIZED field per MAP step first.
///
/// A record the model does not affirm — an unparseable response, a missing row or verdict, any
/// answer other than true/"yes" — is dropped. Only a failed transport throws, failing the query, as
/// in SEM_MAP.
class SemanticFilterPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    /// `inputFields` are the model's declared INPUT fields, `outputFields` its declared OUTPUT
    /// fields, one per MAP step in step order — empty for a plain filter.
    SemanticFilterPhysicalOperator(
        SemanticBackendProvider backendProvider,
        SemanticModelConfig config,
        std::vector<QualifiedIdentifier> inputFields,
        std::vector<QualifiedIdentifier> outputFields);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    /// Behind a shared_ptr because the type-erased PhysicalOperator copies this class by value while
    /// the traced code bakes the state's address in as a constant: the address must stay stable.
    std::shared_ptr<detail::SemanticFilterState> state;
    std::vector<QualifiedIdentifier> inputFields;
    std::vector<QualifiedIdentifier> outputFields;
    std::optional<PhysicalOperator> child;
};

}
