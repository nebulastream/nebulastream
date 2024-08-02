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

#ifndef NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP
#define NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP

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
    friend std::ostream& operator<<(std::ostream& os, const AbstractDataType& type) {
        os << type.toString();
        return os;
    }

    AbstractDataType& operator=(const AbstractDataType& other) {
        null = other.null;
        return *this;
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
    /// These methods should not be called directly, but rather through override operators in ExecDataTypes
    /// We assume that the methods below are called with the same data type, e.g., both u64 or both double
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

    nautilus::val<bool> null;
};

}// namespace NES::Nautilus

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP
