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
namespace NES::QueryCompilation::PhysicalOperators {
class PhysicalGatherSlicesOperator final : public PhysicalUnaryOperator {
public:
  PhysicalGatherSlicesOperator(
    OperatorId id,
    std::shared_ptr<Schema> inputSchema,
    std::shared_ptr<Schema> outputSchema,
    TimestampField timeStampField,
    const uint64_t windowSize,
    const uint64_t windowSlide,
    std::shared_ptr<Runtime::Execution::OperatorHandler> operatorHandler)
    : Operator(id),
      PhysicalUnaryOperator(std::move(id), std::move(inputSchema), std::move(outputSchema))
      , timeStampField(std::move(timeStampField))
      , windowSize(windowSize)
      , windowSlide(windowSlide)
      , operatorHandler(std::move(operatorHandler)) {
  }

  std::shared_ptr<Operator> copy() override {
    return std::make_shared<PhysicalGatherSlicesOperator>(id,
                                                          inputSchema,
                                                          outputSchema,
                                                          timeStampField,
                                                          windowSize,
                                                          windowSlide,
                                                          operatorHandler);
  }

  [[nodiscard]] TimestampField getTimeFunction() const { return timeStampField; }
  [[nodiscard]] uint64_t getWindowSize() const { return windowSize; }
  [[nodiscard]] uint64_t getWindowSlide() const { return windowSlide; }
  [[nodiscard]] std::shared_ptr<Runtime::Execution::OperatorHandler> getOperatorHandler() const { return operatorHandler; }

private:
  TimestampField timeStampField;
  uint64_t windowSize;
  uint64_t windowSlide;
  std::shared_ptr<Runtime::Execution::OperatorHandler> operatorHandler;
};
}
