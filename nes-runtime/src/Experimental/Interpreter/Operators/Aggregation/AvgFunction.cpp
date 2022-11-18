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
#include <Experimental/Interpreter/Operators/Aggregation/AvgFunction.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
namespace NES::Nautilus {

std::unique_ptr<AggregationState> AvgFunction::createGlobalState() { return std::make_unique<GlobalSumState>(); }

std::unique_ptr<AggregationState> AvgFunction::createState() {
    if (auto integerStamp = cast_if<Nautilus::IR::Types::IntegerStamp>(stamp.get())) {
        if (integerStamp->isUnsigned()) {
            switch (integerStamp->getBitWidth()) {
                case Nautilus::IR::Types::IntegerStamp::I8: {
                    return std::make_unique<AvgState>(Value<>((uint8_t) 0), Value<>((int64_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I16: {
                    return std::make_unique<AvgState>(Value<>((uint16_t) 0), Value<>((int64_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I32: {
                    return std::make_unique<AvgState>(Value<>((uint32_t) 0), Value<>((int64_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I64: {
                    return std::make_unique<AvgState>(Value<>((uint64_t) 0), Value<>((int64_t) 0));
                };
            }
        } else {
            switch (integerStamp->getBitWidth()) {
                case Nautilus::IR::Types::IntegerStamp::I8: {
                    return std::make_unique<AvgState>(Value<>((int8_t) 0), Value<>((int64_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I16: {
                    return std::make_unique<AvgState>(Value<>((int16_t) 0), Value<>((int64_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I32: {
                    return std::make_unique<AvgState>(Value<>((int32_t) 0), Value<>((int64_t) 0));
                };
                case Nautilus::IR::Types::IntegerStamp::I64: {
                    return std::make_unique<AvgState>(Value<>((int64_t) 0), Value<>((int64_t) 0));
                };
            }
        }
    } else if (auto floatStamp = cast_if<Nautilus::IR::Types::FloatStamp>(stamp.get())) {
        switch (floatStamp->getBitWidth()) {
            case Nautilus::IR::Types::FloatStamp::F32: {
                return std::make_unique<AvgState>(Value<>(0.0f), Value<>((int64_t) 0));
            };
            case Nautilus::IR::Types::FloatStamp::F64: {
                return std::make_unique<AvgState>(Value<>(0.0), Value<>((int64_t) 0));
            };
        }
    }
    NES_THROW_RUNTIME_ERROR("Sum state on " << stamp->toString() << " is not supported.");
}

AvgFunction::AvgFunction(Runtime::Execution::Expressions::ExpressionPtr expression, Nautilus::IR::Types::StampPtr stamp)
    : expression(expression), stamp(stamp) {}

void AvgFunction::liftCombine(std::unique_ptr<AggregationState>& state, Record& record) {
    auto sumState = (AvgState*) state.get();
    auto value = expression->execute(record);
    sumState->sum = sumState->sum + value;
    sumState->count = sumState->count + (int64_t) 1;
}

void AvgFunction::combine(std::unique_ptr<AggregationState>& leftState, std::unique_ptr<AggregationState>& rightState) {
    auto leftSum = (AvgState*) leftState.get();
    auto rightSum = (AvgState*) rightState.get();
    leftSum->count = leftSum->count + rightSum->count;
    leftSum->sum = leftSum->sum + rightSum->sum;
}

Value<Any> AvgFunction::lower(std::unique_ptr<AggregationState>& state) {
    auto sumState = (AvgState*) state.get();
    return sumState->sum / sumState->count;
}
std::unique_ptr<AggregationState> AvgFunction::loadState(Value<MemRef>& ref) {
    auto counter = ref.load<Int64>();
    auto valueRef = (ref + (uint64_t) 8).as<MemRef>();
    if (auto integerStamp = cast_if<IR::Types::IntegerStamp>(stamp.get())) {
        if (integerStamp->isUnsigned()) {
            switch (integerStamp->getBitWidth()) {
                case Nautilus::IR::Types::IntegerStamp::I8: {
                    return std::make_unique<AvgState>(valueRef.load<UInt8>(), counter);
                };
                case Nautilus::IR::Types::IntegerStamp::I16: {
                    return std::make_unique<AvgState>(valueRef.load<UInt16>(), counter);
                };
                case Nautilus::IR::Types::IntegerStamp::I32: {
                    return std::make_unique<AvgState>(valueRef.load<UInt32>(), counter);
                };
                case Nautilus::IR::Types::IntegerStamp::I64: {
                    return std::make_unique<AvgState>(valueRef.load<UInt64>(), counter);
                };
            }
        } else {
            switch (integerStamp->getBitWidth()) {
                case Nautilus::IR::Types::IntegerStamp::I8: {
                    return std::make_unique<AvgState>(valueRef.load<Int8>(), counter);
                };
                case Nautilus::IR::Types::IntegerStamp::I16: {
                    return std::make_unique<AvgState>(valueRef.load<Int16>(), counter);
                };
                case Nautilus::IR::Types::IntegerStamp::I32: {
                    return std::make_unique<AvgState>(valueRef.load<Int32>(), counter);
                };
                case Nautilus::IR::Types::IntegerStamp::I64: {
                    return std::make_unique<AvgState>(valueRef.load<Int64>(), counter);
                };
            }
        }
    } else if (auto floatStamp = cast_if<Nautilus::IR::Types::FloatStamp>(stamp.get())) {
        switch (floatStamp->getBitWidth()) {
            case Nautilus::IR::Types::FloatStamp::F32: {
                return std::make_unique<AvgState>(valueRef.load<Float>(), counter);
            };
            case Nautilus::IR::Types::FloatStamp::F64: {
                return std::make_unique<AvgState>(valueRef.load<Double>(), counter);
            };
        }
    }
    NES_THROW_RUNTIME_ERROR("Sum state on " << stamp->toString() << " is not supported.");
}
void AvgFunction::storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) {
    auto sumState = (AvgState*) state.get();
    ref.store(sumState->count);
    auto valueRef = (ref + (uint64_t) 8).as<MemRef>();
    valueRef.store(sumState->sum);
}
uint64_t AvgFunction::getStateSize() const { return 16; }
}// namespace NES::Nautilus