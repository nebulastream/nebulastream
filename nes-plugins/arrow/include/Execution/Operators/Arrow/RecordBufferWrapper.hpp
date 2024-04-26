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

#ifndef NES_PLUGINS_ARROW_INCLUDE_EXECUTION_OPERATORS_ARROW_RECORDBUFFERWRAPPER_HPP_
#define NES_PLUGINS_ARROW_INCLUDE_EXECUTION_OPERATORS_ARROW_RECORDBUFFERWRAPPER_HPP_
#include <memory>
namespace arrow {
class RecordBatch;
}
namespace NES::Runtime::Execution::Operators {
struct RecordBufferWrapper {
    RecordBufferWrapper(std::shared_ptr<arrow::RecordBatch> batch) : batch(batch) {}
    std::shared_ptr<arrow::RecordBatch> batch;
};

}// namespace NES::Runtime::Execution::Operators

#endif // NES_PLUGINS_ARROW_INCLUDE_EXECUTION_OPERATORS_ARROW_RECORDBUFFERWRAPPER_HPP_
