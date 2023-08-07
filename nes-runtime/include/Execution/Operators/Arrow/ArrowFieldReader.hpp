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
#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_ARROW_ARROWFIELDREADER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_ARROW_ARROWFIELDREADER_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Operator.hpp>
#include <arrow/type_fwd.h>
#include <concepts>
#include <type_traits>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Abstract field reader for arrow.
 * The field reader accesses a column in an RecordBatch and provides access to individual fields.
 */
class AbstractArrowFieldReader {
  public:
    AbstractArrowFieldReader(const uint64_t fieldIndex, const Record::RecordFieldIdentifier& fieldName);
    virtual Value<> getColumn(const Value<MemRef>& recordBatch) = 0;
    virtual Value<> getValue(const Value<MemRef>& column, const Value<UInt64>& index) = 0;
    virtual ~AbstractArrowFieldReader() = default;

    const uint64_t fieldIndex;
    const Record::RecordFieldIdentifier fieldName;
};

template<typename T>
concept HasRawValues = requires(T* t) { t->raw_values(); };

template<typename ArrowType>
class ArrowFieldReader : public AbstractArrowFieldReader {
  public:
    ArrowFieldReader(const uint64_t fieldIndex, const Record::RecordFieldIdentifier& fieldName);
    Value<> getColumn(const Value<MemRef>& recordBatch) override;
    Value<> getValue(const Value<MemRef>& column, const Value<UInt64>& index) override;
};

/**
 * @brief Create a vector of arrow field readers from a schema.
 * @param schema
 * @return std::vector<std::shared_ptr<AbstractArrowFieldReader>>
 */
std::vector<std::shared_ptr<AbstractArrowFieldReader>> createArrowFieldReaderFromSchema(const SchemaPtr& schema);
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_ARROW_ARROWFIELDREADER_HPP_
