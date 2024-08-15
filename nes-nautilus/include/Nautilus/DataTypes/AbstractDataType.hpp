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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP_

#include <Util/Logger/Logger.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Nautilus {

class AbstractDataType;
using ExecDataType = std::shared_ptr<AbstractDataType>;

class AbstractDataType : public std::enable_shared_from_this<AbstractDataType> {
  public:
    explicit AbstractDataType(const nautilus::val<bool>& null) : null(null) {}
    virtual ~AbstractDataType() = default;

    friend std::ostream& operator<<(std::ostream& os, const ExecDataType& type) {
        os << type->toString();
        return os;
    }

    AbstractDataType& operator=(const AbstractDataType& other) {
        null = other.null;
        return *this;
    }

    explicit operator bool() const {
        return !null && isBool();
    }

    [[nodiscard]] const nautilus::val<bool>& isNull() const { return null; }

    template<class DataType>
    bool instanceOf() {
        return dynamic_cast<DataType*>(this);
    }

    template<class DataType>
    std::shared_ptr<DataType> as() {
        if (instanceOf<DataType>()) {
            return std::dynamic_pointer_cast<DataType>(this->shared_from_this());
        }
        throw Exceptions::RuntimeException("DataType:: we performed an invalid cast of operator to type "
                                           + std::string(typeid(DataType).name()));
    }

    friend ExecDataType operator+(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator&&(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator||(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator==(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator!=(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator<(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator>(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator<=(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator>=(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator-(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator*(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator/(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator%(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator&(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator|(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator^(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator<<(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator>>(const ExecDataType& lhs, const ExecDataType& rhs);
    friend ExecDataType operator!(const ExecDataType& lhs);

  protected:
    /// Defining operations on data types with other ExecDataTypes
    /// These methods should not be called directly, but rather through overridden operators in ExecutableDataTypeOperations
    /// We assume that the methods below are called with the same data type, e.g., both u64 or both double
    /// Otherwise, an error is thrown
    virtual ExecDataType operator&&(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator||(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator==(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator!=(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator<(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator>(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator<=(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator>=(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator+(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator-(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator*(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator/(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator%(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator&(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator|(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator^(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator<<(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator>>(const ExecDataType& rightExp) const = 0;
    virtual ExecDataType operator!() const = 0;


    [[nodiscard]] virtual std::string toString() const = 0;
    virtual nautilus::val<bool> isBool() const = 0;

    nautilus::val<bool> null;
};

/// Define common nautilus data types and their C++ counterparts
template<typename Struct_Class_Name>
using ObjRefVal = nautilus::val<Struct_Class_Name*>; template<typename Struct_Class_Name> using ObjRef = Struct_Class_Name*;
using MemRefVal = nautilus::val<int8_t*>; using MemRef = int8_t*;
using Int8Val = nautilus::val<int8_t>; using Int8 = int8_t;
using Int16Val = nautilus::val<int16_t>; using Int16 = int16_t;
using Int32Val = nautilus::val<int32_t>; using Int32 = int32_t;
using Int64Val = nautilus::val<int64_t>; using Int64 = int64_t;
using UInt8Val = nautilus::val<uint8_t>; using UInt8 = uint8_t;
using UInt16Val = nautilus::val<uint16_t>; using UInt16 = uint16_t;
using UInt32Val = nautilus::val<uint32_t>; using UInt32 = uint32_t;
using UInt64Val = nautilus::val<uint64_t>; using UInt64 = uint64_t;
using FloatVal = nautilus::val<float>; using Float = float;
using DoubleVal = nautilus::val<double>; using Double = double;
using BooleanVal = nautilus::val<bool>; using Boolean = bool;


/**
* @brief Get member returns the MemRef to a specific class member as an offset to a objectReference.
* @note This assumes the offsetof works for the classType.
* @param objectReference reference to the object that contains the member.
* @param classType type of a class or struct
* @param member a member that is part of the classType
*/
#define getMember(objectReference, classType, member, dataType)                                                                  \
(static_cast<nautilus::val<dataType>>(*static_cast<nautilus::val<dataType*>>(                                                \
objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member))))))
#define getMemberAsFixedSizeExecutableDataType(objectReference, classType, member, dataType)                                                    \
(std::dynamic_pointer_cast<FixedSizeExecutableDataType<dataType>>(                                                                    \
FixedSizeExecutableDataType<dataType>::create(*static_cast<nautilus::val<dataType*>>(                                             \
objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member)))))))
#define getMemberAsPointer(objectReference, classType, member, dataType)                                                         \
(static_cast<nautilus::val<dataType*>>(objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member)))))

#define writeValueToMemRef(memRef, value, dataType) (*static_cast<nautilus::val<dataType*>>(memRef) = value)
#define readValueFromMemRef(memRef, dataType)                                                                                    \
(static_cast<nautilus::val<dataType>>(*static_cast<nautilus::val<dataType*>>(memRef)))


BooleanVal memEquals(const MemRefVal& ptr1, const MemRefVal& ptr2, const nautilus::val<uint64_t>& size);
void memCopy(const MemRefVal& dest, const MemRefVal& src, const nautilus::val<size_t>& size);
}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP_
