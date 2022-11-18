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
#include <Nautilus/IR/Types/AddressStamp.hpp>
#include <Nautilus/IR/Types/ArrayStamp.hpp>
#include <Nautilus/IR/Types/BooleanStamp.hpp>
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/IR/Types/VoidStamp.hpp>

namespace NES::Nautilus::IR::Types {

StampPtr StampFactory::createVoidStamp() { return std::make_shared<VoidStamp>(); }

StampPtr StampFactory::createAddressStamp() { return std::make_shared<AddressStamp>(); }

StampPtr StampFactory::createBooleanStamp() { return std::make_shared<BooleanStamp>(); }

StampPtr StampFactory::createUInt8Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I8, IntegerStamp::SignednessSemantics::Unsigned);
}

StampPtr StampFactory::createUInt16Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I16, IntegerStamp::SignednessSemantics::Unsigned);
}

StampPtr StampFactory::createUInt32Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I32, IntegerStamp::SignednessSemantics::Unsigned);
}

StampPtr StampFactory::createUInt64Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I64, IntegerStamp::SignednessSemantics::Unsigned);
}

StampPtr StampFactory::createInt8Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I8, IntegerStamp::SignednessSemantics::Signed);
}

StampPtr StampFactory::createInt16Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I16, IntegerStamp::SignednessSemantics::Signed);
}

StampPtr StampFactory::createInt32Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I32, IntegerStamp::SignednessSemantics::Signed);
}

StampPtr StampFactory::createInt64Stamp() {
    return std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I64, IntegerStamp::SignednessSemantics::Signed);
}

StampPtr StampFactory::createFloatStamp() { return std::make_shared<FloatStamp>(FloatStamp::BitWidth::F32); }

StampPtr StampFactory::createDoubleStamp() { return std::make_shared<FloatStamp>(FloatStamp::BitWidth::F64); }

StampPtr StampFactory::createArrayStamp(uint64_t size, StampPtr component) {
    return std::make_shared<ArrayStamp>(size, component);
}

}// namespace NES::Nautilus::IR::Types
