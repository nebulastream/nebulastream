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
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
class PhysicalSortTuplesInBuffer final : public AbstractScanOperator, public PhysicalUnaryOperator {
public:
  PhysicalSortTuplesInBuffer(
    OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema,
    std::string sort_field)
    : Operator(id), PhysicalUnaryOperator(std::move(id), std::move(inputSchema), std::move(outputSchema))
      , sortField(std::move(sort_field)) {
  }

  std::shared_ptr<Operator> copy() override {
    return std::make_shared<PhysicalSortTuplesInBuffer>(id, inputSchema, outputSchema, sortField);
  }

  std::shared_ptr<Runtime::Execution::Interface::MemoryProvider::TupleBufferMemoryProvider> getMemoryProviderInput(
    uint64_t bufferSize) const {
    auto schema = this->getOutputSchema();
    INVARIANT(
      schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT,
      "Currently only row layout is supported, current layout={}",
      magic_enum::enum_name(schema->getLayoutType()));
    /// pass buffer size here
    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
        = std::make_shared<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
    return memoryProvider;
  }
  [[nodiscard]] std::string getSortField() const { return sortField; }

private:
  std::string sortField;
};
}
