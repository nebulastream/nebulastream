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

#include <UnionRenamePhysicalOperator.hpp>

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifier.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <static.hpp>

namespace NES
{

void UnionRenamePhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    Record outputRecord;
    for (nautilus::static_val<size_t> i = 0; i < outputFields.size(); ++i)
    {
        outputRecord.write(outputFields.at(i), record.read(identifierMap.at(outputFields.at(i))));
    }
    executeChild(ctx, outputRecord);
}

UnionRenamePhysicalOperator::UnionRenamePhysicalOperator(
    const std::vector<QualifiedIdentifier>& inputFields, std::vector<QualifiedIdentifier> outputFields)
    : outputFields(std::move(outputFields))
{
    INVARIANT(inputFields.size() == this->outputFields.size(), "Input and output fields must have the same size");

    for (const auto& inputField : inputFields)
    {
        for (const auto& outputField : this->outputFields)
        {
            if (inputField == outputField)
            {
                PRECONDITION(!identifierMap.contains(inputField), "Input field cannot be mapped twice");
                identifierMap.emplace(outputField, inputField);
            }
        }
    }

    INVARIANT(identifierMap.size() == inputFields.size(), "All input fields must be mapped");
}

std::optional<PhysicalOperator> UnionRenamePhysicalOperator::getChild() const
{
    return child;
}

void UnionRenamePhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
