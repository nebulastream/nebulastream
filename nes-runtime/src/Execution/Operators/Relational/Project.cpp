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

#include <Execution/Operators/Relational/Project.hpp>
#include <Nautilus/Interface/Record.hpp>
namespace NES::Runtime::Execution::Operators {

void Project::execute(ExecutionContext& ctx, Record& record) const {
    // If the number of inputFields and outputFields match, map inputFields to outputFields.
    // Todo: 4068: We plan to use maps to support renaming, reordering and reducing the number of attributes
    //             using projections at the same time (Currently, we cannot e.g. reduce and rename at the same time).
    if (inputFields.size() == outputFields.size()) {
        Record projectedRecord;
        for (size_t index = 0; index < record.numberOfFields(); ++index) {
            auto inputName = inputFields.at(index);
            auto value = record.read(inputName);
            auto newName = outputFields.at(index);
            projectedRecord.write(newName, value);
        }
        // call next operator
        child->execute(ctx, projectedRecord);
    } else {
        // If the number of input and output fields do not match, simply relay the record to the next operator.
        child->execute(ctx, record);
    }
}
}// namespace NES::Runtime::Execution::Operators