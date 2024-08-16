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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <Nautilus/DataTypes/ExecutableDataTypesConcepts.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <google/protobuf/stubs/port.h>
#include <nautilus/std/string.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ostream>
#include <sstream>

namespace NES::Nautilus {

/// Forward declaration, so that we can define this method as a friend in FixedSizeExecutableDataType
template<FixedDataTypes CPP_DataType>
auto castAndLoadValue(const ExecDataType& executableDataType);

template<typename ValueType>
class FixedSizeExecutableDataType : public AbstractDataType {
  public:
    explicit FixedSizeExecutableDataType(const nautilus::val<ValueType>& value, const nautilus::val<bool>& null)
        : AbstractDataType(null), rawValue(value) {}

    static ExecDataType create(nautilus::val<ValueType> value, nautilus::val<bool> null = false) {
        return std::make_shared<FixedSizeExecutableDataType<ValueType>>(value, null);
    }

    void writeToMemRefVal(const MemRefVal& memRef) override;
    ~FixedSizeExecutableDataType() override = default;

    /// We make the following two methods a friend so that we can call valueAsType() from them
    template<FixedDataTypes CPP_DataType>
    friend auto castAndLoadValue(const ExecDataType& executableDataType);
    template<typename CastType>
    friend ExecDataType castTo(const ExecDataType& lhs);

  protected:
    // Implementing operations on data types
    ExecDataType operator&&(const ExecDataType& rightExp) const override;
    ExecDataType operator||(const ExecDataType& rightExp) const override;
    ExecDataType operator==(const ExecDataType& rightExp) const override;
    ExecDataType operator!=(const ExecDataType& rightExp) const override;
    ExecDataType operator<(const ExecDataType& rightExp) const override;
    ExecDataType operator>(const ExecDataType& rightExp) const override;
    ExecDataType operator<=(const ExecDataType& rightExp) const override;
    ExecDataType operator>=(const ExecDataType& rightExp) const override;
    ExecDataType operator+(const ExecDataType& rightExp) const override;
    ExecDataType operator-(const ExecDataType& rightExp) const override;
    ExecDataType operator*(const ExecDataType& rightExp) const override;
    ExecDataType operator/(const ExecDataType& rightExp) const override;
    ExecDataType operator%(const ExecDataType& rightExp) const override;
    ExecDataType operator&(const ExecDataType& rightExp) const override;
    ExecDataType operator|(const ExecDataType& rightExp) const override;
    ExecDataType operator^(const ExecDataType& rightExp) const override;
    ExecDataType operator<<(const ExecDataType& rightExp) const override;
    ExecDataType operator>>(const ExecDataType& rightExp) const override;
    ExecDataType operator!() const override;

    /// This should the only way to get the value of the data type.
    /// This method should NOT be made public, rather used via castAndLoadValue()
    template<typename CastedDataType>
    nautilus::val<CastedDataType> valueAsType() const {
        if constexpr (std::is_same_v<CastedDataType, ValueType>) {
            return rawValue;
        } else {
            return static_cast<nautilus::val<CastedDataType>>(rawValue);
        }
    }

    std::string toString() const override;
    nautilus::val<bool> isBool() const override;

    nautilus::val<ValueType> rawValue;
};


// Define common ExecutableDataTypes
template<typename ValueType>
using FixedSizeExecutableDataTypePtr = std::shared_ptr<FixedSizeExecutableDataType<ValueType>>;
using ExecDataI8 = FixedSizeExecutableDataTypePtr<Int8>;
using ExecDataI16 = FixedSizeExecutableDataTypePtr<Int16>;
using ExecDataI32 = FixedSizeExecutableDataTypePtr<Int32>;
using ExecDataI64 = FixedSizeExecutableDataTypePtr<Int64>;
using ExecDataUI8 = FixedSizeExecutableDataTypePtr<UInt8>;
using ExecDataUI16 = FixedSizeExecutableDataTypePtr<UInt16>;
using ExecDataUI32 = FixedSizeExecutableDataTypePtr<UInt32>;
using ExecDataUI64 = FixedSizeExecutableDataTypePtr<UInt64>;
using ExecDataF32 = FixedSizeExecutableDataTypePtr<Float>;
using ExecDataF64 = FixedSizeExecutableDataTypePtr<Double>;
using ExecDataBool = FixedSizeExecutableDataTypePtr<Boolean>;

ExecDataType readExecDataTypeFromMemRef(MemRefVal& memRef, const PhysicalTypePtr& type);
std::pair<ExecDataType, ExecDataType> castToSameType(const ExecDataType& lhs, const ExecDataType& rhs);

template<typename CastType>
ExecDataType castTo(const ExecDataType& lhs) {
    if (lhs->instanceOf<FixedSizeExecutableDataType<UInt8>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<UInt8>>()->valueAsType<uint8_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<UInt16>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<UInt16>>()->valueAsType<uint16_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<UInt32>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<UInt32>>()->valueAsType<uint32_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<UInt64>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<UInt64>>()->valueAsType<uint64_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<Int8>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<Int8>>()->valueAsType<int8_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<Int16>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<Int16>>()->valueAsType<int16_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<Int32>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<Int32>>()->valueAsType<int32_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<Int64>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<Int64>>()->valueAsType<int64_t>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<Float>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<Float>>()->valueAsType<float>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<Double>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<Double>>()->valueAsType<double>()));
    } else if (lhs->instanceOf<FixedSizeExecutableDataType<Boolean>>()) {
        return FixedSizeExecutableDataType<CastType>::create(
            static_cast<nautilus::val<CastType>>(lhs->as<FixedSizeExecutableDataType<Boolean>>()->valueAsType<bool>()));
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

template<FixedDataTypes CPP_DataType>
auto castAndLoadValue(const ExecDataType& executableDataType) {
    return castTo<CPP_DataType>(executableDataType)
        ->template as<FixedSizeExecutableDataType<CPP_DataType>>()
        ->template valueAsType<CPP_DataType>();
}

}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
