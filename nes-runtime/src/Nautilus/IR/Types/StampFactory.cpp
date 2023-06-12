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

static  StampPtr VOID_STAMP = std::make_shared<VoidStamp>();
static const StampPtr ADDRESS_STAMP = std::make_shared<AddressStamp>();
static const StampPtr BOOLEAN_STAMP = std::make_shared<BooleanStamp>();
static const StampPtr UINT8_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I8, IntegerStamp::SignednessSemantics::Unsigned);
static const StampPtr UINT16_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I16, IntegerStamp::SignednessSemantics::Unsigned);
static const StampPtr UINT32_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I32, IntegerStamp::SignednessSemantics::Unsigned);
static const StampPtr UINT64_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I64, IntegerStamp::SignednessSemantics::Unsigned);
static const StampPtr INT8_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I8, IntegerStamp::SignednessSemantics::Signed);
static const StampPtr INT16_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I16, IntegerStamp::SignednessSemantics::Signed);
static const StampPtr INT32_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I32, IntegerStamp::SignednessSemantics::Signed);
static const StampPtr INT64_STAMP =
    std::make_shared<IntegerStamp>(IntegerStamp::BitWidth::I64, IntegerStamp::SignednessSemantics::Signed);
static const StampPtr FLOAT_STAMP = std::make_shared<FloatStamp>(FloatStamp::BitWidth::F32);
static const StampPtr DOUBLE_STAMP = std::make_shared<FloatStamp>(FloatStamp::BitWidth::F64);

StampPtr StampFactory::createVoidStamp() { return VOID_STAMP; }

StampPtr StampFactory::createAddressStamp() { return ADDRESS_STAMP; }

StampPtr StampFactory::createBooleanStamp() { return BOOLEAN_STAMP; }

StampPtr StampFactory::createUInt8Stamp() { return UINT8_STAMP; }

StampPtr StampFactory::createUInt16Stamp() { return UINT16_STAMP; }

StampPtr StampFactory::createUInt32Stamp() { return UINT32_STAMP; }

StampPtr StampFactory::createUInt64Stamp() { return UINT64_STAMP; }

StampPtr StampFactory::createInt8Stamp() { return INT8_STAMP; }

StampPtr StampFactory::createInt16Stamp() { return INT16_STAMP; }

StampPtr StampFactory::createInt32Stamp() { return INT32_STAMP; }

StampPtr StampFactory::createInt64Stamp() { return INT64_STAMP; }

StampPtr StampFactory::createFloatStamp() { return FLOAT_STAMP; }

StampPtr StampFactory::createDoubleStamp() { return DOUBLE_STAMP; }

StampPtr StampFactory::createArrayStamp(uint64_t size, StampPtr component) {
    return std::make_shared<ArrayStamp>(size, component);
}

}// namespace NES::Nautilus::IR::Types
