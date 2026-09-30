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

#include <Interface/Record.hpp>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <CompilationContext.hpp>
#include <PhysicalOperator.hpp>
#include <SemanticBackend.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES::detail
{
struct SemanticMapState;
}

namespace NES
{

/// Evaluates SEM_MAP record by record with one blocking LLM round trip each (stage 1). The record
/// passes through unchanged and gains one VARSIZED field per step.
///
/// Every record leaves the operator: an unparseable response or an answer outside the declared
/// values writes the step's default value. Only a failed transport (endpoint unreachable or non-2xx
/// after all retries) throws, failing the query, because that is the deployment's problem rather
/// than the record's.
class SemanticMapPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    /// `inputFields` are the model's declared INPUT fields, `outputFields` its declared OUTPUT fields
    /// in step order. Under the table-valued SEM_MAP(model, input) syntax the record fields carry
    /// exactly these names, so one list per side is enough.
    SemanticMapPhysicalOperator(
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
    std::shared_ptr<detail::SemanticMapState> state;
    std::vector<QualifiedIdentifier> inputFields;
    std::vector<QualifiedIdentifier> outputFields;
    std::optional<PhysicalOperator> child;
};

}
