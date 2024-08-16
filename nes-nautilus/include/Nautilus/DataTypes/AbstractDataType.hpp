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

/// Define common nautilus data types and their C++ counterparts
template<typename Struct_Class_Name>
using ObjRefVal = nautilus::val<Struct_Class_Name*>;
template<typename Struct_Class_Name>
using ObjRef = Struct_Class_Name*;
using MemRefVal = nautilus::val<int8_t*>;
using MemRef = int8_t*;
using Int8Val = nautilus::val<int8_t>;
using Int8 = int8_t;
using Int16Val = nautilus::val<int16_t>;
using Int16 = int16_t;
using Int32Val = nautilus::val<int32_t>;
using Int32 = int32_t;
using Int64Val = nautilus::val<int64_t>;
using Int64 = int64_t;
using UInt8Val = nautilus::val<uint8_t>;
using UInt8 = uint8_t;
using UInt16Val = nautilus::val<uint16_t>;
using UInt16 = uint16_t;
using UInt32Val = nautilus::val<uint32_t>;
using UInt32 = uint32_t;
using UInt64Val = nautilus::val<uint64_t>;
using UInt64 = uint64_t;
using FloatVal = nautilus::val<float>;
using Float = float;
using DoubleVal = nautilus::val<double>;
using Double = double;
using BooleanVal = nautilus::val<bool>;
using Boolean = bool;


class AbstractDataType;
using ExecDataType = std::shared_ptr<AbstractDataType>;

/// This class is the base class for all data types in NebulaStream.
/// It sits on top of the nautilus library and its val<> data type.
/// We derive all specific data types, e.g., variable and fixed data types, from this base class.
/// This class provides all functionality so that a developer does not have to know of any nautilus::val<> and how to work with them.
///
/// As we want to support null handling, this class has a `val<bool> null` member.
/// Even though, we have here a null member, we do not support end-to-end null handling.
/// All operations on AbstractDataTypes are implemented with null handling in mind, but
/// we currently do not check for potential null values in any operator.
class AbstractDataType : public std::enable_shared_from_this<AbstractDataType> {
  public:
    explicit AbstractDataType(const nautilus::val<bool>& null) : null(null) {}
    virtual ~AbstractDataType() = default;

    /// Overloaded the << operator so that we can easily create a string or use it in our logging.
    /// Under the hood, we call the toString() method
    friend std::ostream& operator<<(std::ostream& os, const ExecDataType& type);
    friend std::ostream& operator<<(std::ostream& os, const AbstractDataType& type);
    AbstractDataType& operator=(const AbstractDataType& other);
    explicit operator bool() const;
    [[nodiscard]] const nautilus::val<bool>& isNull() const;

    /// Writes its value to the memRef.
    virtual void writeToMemRefVal(const MemRefVal& memRef) = 0;

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


    /// Declaring friend functions for operations on data types, so that we can access private methods in these methods
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

    /// Every child class implements their own custom toString() method.
    /// This method gets called when the format operator<< is called for the AbstractDataType
    [[nodiscard]] virtual std::string toString() const = 0;

    /// Every child class implements their own custom isBool() method.
    /// This method gets called once a data type is checked if it is a boolean, i.e., in an if-statement.
    [[nodiscard]]
    virtual nautilus::val<bool> isBool() const = 0;

    nautilus::val<bool> null;
};


/// A wrapper around std::memcmp() and checks if ptr1 and ptr2 are equal
BooleanVal memEquals(const MemRefVal& ptr1, const MemRefVal& ptr2, const nautilus::val<uint64_t>& size);

/// A wrapper around std::memcpy()
void memCopy(const MemRefVal& dest, const MemRefVal& src, const nautilus::val<size_t>& size);
}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP_
