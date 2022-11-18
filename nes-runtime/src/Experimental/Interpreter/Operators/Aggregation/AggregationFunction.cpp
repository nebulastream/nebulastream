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

#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>

using namespace NES::Nautilus;
namespace NES::Nautilus {

std::unique_ptr<AggregationState> SumFunction::createGlobalState() { return std::make_unique<GlobalSumState>(); }

std::unique_ptr<AggregationState> SumFunction::createState() {
    if (auto integerStamp = cast_if<Nautilus::IR::Types::IntegerStamp>(stamp.get())) {
        if (integerStamp->isUnsigned()) {
            switch (integerStamp->getBitWidth()) {
                case Nautilus::IR::Types::IntegerStamp::I8: {
                    return std::make_unique<SumState>(Value<>((uint8_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I16: {
                    return std::make_unique<SumState>(Value<>((uint16_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I32: {
                    return std::make_unique<SumState>(Value<>((uint32_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I64: {
                    return std::make_unique<SumState>(Value<>((uint64_t) 0));
                };
            }
        } else {
            switch (integerStamp->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: {
                    return std::make_unique<SumState>(Value<>((int8_t) 0));
                };
                case IR::Types::IntegerStamp::I16: {
                    return std::make_unique<SumState>(Value<>((int16_t) 0));
                };
                case IR::Types::IntegerStamp::I32: {
                    return std::make_unique<SumState>(Value<>((int32_t) 0));
                };
                case IR::Types::IntegerStamp::I64: {
                    return std::make_unique<SumState>(Value<>((int64_t) 0));
                };
            }
        }
    } else if (auto floatStamp = cast_if<IR::Types::FloatStamp>(stamp.get())) {
        switch (floatStamp->getBitWidth()) {
            case IR::Types::FloatStamp::F32: {
                return std::make_unique<SumState>(Value<>(0.0f));
            };
            case IR::Types::FloatStamp::F64: {
                return std::make_unique<SumState>(Value<>(0.0));
            };
        }
    }
    NES_THROW_RUNTIME_ERROR("Sum state on " << stamp->toString() << " is not supported.");
}

SumFunction::SumFunction(Runtime::Execution::Expressions::ExpressionPtr expression, IR::Types::StampPtr stamp)
    : expression(expression), stamp(stamp) {}

void SumFunction::liftCombine(std::unique_ptr<AggregationState>& state, Record& record) {
    auto sumState = (SumState*) state.get();
    auto value = expression->execute(record);
    sumState->currentSum = sumState->currentSum + value;
}

void SumFunction::combine(std::unique_ptr<AggregationState>& leftState, std::unique_ptr<AggregationState>& rightState) {
    auto leftSum = (SumState*) leftState.get();
    auto rightSum = (SumState*) rightState.get();
    leftSum->currentSum = leftSum->currentSum + rightSum->currentSum;
}

Value<Any> SumFunction::lower(std::unique_ptr<AggregationState>& state) {
    auto sumState = (SumState*) state.get();
    return sumState->currentSum;
}
std::unique_ptr<AggregationState> SumFunction::loadState(Value<MemRef>& ref) {
    if (auto integerStamp = cast_if<IR::Types::IntegerStamp>(stamp.get())) {
        if (integerStamp->isUnsigned()) {
            switch (integerStamp->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: {
                    return std::make_unique<SumState>(ref.load<UInt8>());
                };
                case IR::Types::IntegerStamp::I16: {
                    return std::make_unique<SumState>(ref.load<UInt16>());
                };
                case IR::Types::IntegerStamp::I32: {
                    return std::make_unique<SumState>(ref.load<UInt32>());
                };
                case IR::Types::IntegerStamp::I64: {
                    return std::make_unique<SumState>(ref.load<UInt64>());
                };
            }
        } else {
            switch (integerStamp->getBitWidth()) {
                case IR::Types::IntegerStamp::I8: {
                    return std::make_unique<SumState>(ref.load<Int8>());
                };
                case IR::Types::IntegerStamp::I16: {
                    return std::make_unique<SumState>(ref.load<Int16>());
                };
                case IR::Types::IntegerStamp::I32: {
                    return std::make_unique<SumState>(ref.load<Int32>());
                };
                case IR::Types::IntegerStamp::I64: {
                    return std::make_unique<SumState>(ref.load<Int64>());
                };
            }
        }
    } else if (auto floatStamp = cast_if<IR::Types::FloatStamp>(stamp.get())) {
        switch (floatStamp->getBitWidth()) {
            case IR::Types::FloatStamp::F32: {
                return std::make_unique<SumState>(ref.load<Float>());
            };
            case IR::Types::FloatStamp::F64: {
                return std::make_unique<SumState>(ref.load<Double>());
            };
        }
    }
    NES_THROW_RUNTIME_ERROR("Sum state on " << stamp->toString() << " is not supported.");
}
void SumFunction::storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) {
    auto sumState = (SumState*) state.get();
    ref.store(sumState->currentSum);
}
uint64_t SumFunction::getStateSize() const { return 8; }

std::unique_ptr<AggregationState> CountFunction::createGlobalState() { return std::make_unique<GlobalCountState>(); }

std::unique_ptr<AggregationState> CountFunction::createState() { return std::make_unique<CountState>(Value<>((int64_t) 0)); }

CountFunction::CountFunction() {}

void CountFunction::liftCombine(std::unique_ptr<AggregationState>& state, Record&) {
    auto countState = (CountState*) state.get();
    countState->count = countState->count + (int64_t) 1;
}

Value<Any> CountFunction::lower(std::unique_ptr<AggregationState>& ctx) {
    auto countState = (CountState*) ctx.get();
    return countState->count;
}

std::unique_ptr<AggregationState> CountFunction::loadState(Value<MemRef>& ref) {
    return std::make_unique<CountState>(ref.load<Int64>());
}

void CountFunction::storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) {
    auto countState = (CountState*) state.get();
    ref.store(countState->count);
}

void CountFunction::combine(std::unique_ptr<AggregationState>& ctx1, std::unique_ptr<AggregationState>& ctx2) {
    auto countState1 = (CountState*) ctx1.get();
    auto countState2 = (CountState*) ctx2.get();
    countState1->count = countState1->count + countState2->count;
}
uint64_t CountFunction::getStateSize() const { return 8; }

}// namespace NES::Nautilus