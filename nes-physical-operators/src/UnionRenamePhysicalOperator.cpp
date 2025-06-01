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
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <static.hpp>

namespace NES
{

void UnionRenamePhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    Record outputRecord;
    for (nautilus::static_val<size_t> i = 0; i < inputFields.size(); ++i)
    {
        outputRecord.write(outputFields[i], record.read(inputFields[i]));
    }
    executeChild(ctx, outputRecord);
}

UnionRenamePhysicalOperator::UnionRenamePhysicalOperator(std::vector<std::string> inputFields, std::vector<std::string> outputFields)
    : inputFields(std::move(inputFields)), outputFields(std::move(outputFields))
{
    PRECONDITION(this->inputFields.size() == this->outputFields.size(), "Input and output fields must have the same size");
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
