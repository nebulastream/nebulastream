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

#ifndef NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
#define NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_

#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ostream>

namespace NES::Nautilus {

class AbstractDataType;
using ExecDataType = std::shared_ptr<AbstractDataType>;


class AbstractDataType {
  public:
    explicit AbstractDataType(const nautilus::val<bool>& null) : null(null) {}
    virtual ~AbstractDataType() = default;
    friend std::ostream& operator<<(std::ostream& os, const AbstractDataType& type) {
        os << type.toString();
        return os;
    }

    [[nodiscard]] const nautilus::val<bool>& isNull() const { return null; }

    // Defining operations on data types
    virtual ExecDataType operator&&(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator||(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator==(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator!=(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator<(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator>(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator<=(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator>=(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator+(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator-(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator*(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator/(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator%(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator&(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator|(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator^(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator<<(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator>>(const AbstractDataType& rightExp) const = 0;
    virtual ExecDataType operator!() const = 0;

    template<typename CastedDataType>
    bool isType() { return false; };

    template<typename CastedDataType>
    nautilus::val<CastedDataType> as() { NES_NOT_IMPLEMENTED(); return {}; }

    protected:
    [[nodiscard]] virtual std::string toString() const = 0;

    const nautilus::val<bool> null;
};


template<typename ValueType>
class ExecutableDataType : public AbstractDataType {
  public:
    explicit ExecutableDataType(const nautilus::val<ValueType>& value, const nautilus::val<bool>& null)
        : AbstractDataType(null), rawValue(value) {}
    static ExecDataType create(nautilus::val<ValueType> value, bool null = false) {
        return std::make_shared<ExecutableDataType<ValueType>>(value, null);
    }

//    // Implementing operations on data types
    ExecDataType operator&&(const AbstractDataType& rightExp) const override;
    ExecDataType operator||(const AbstractDataType& rightExp) const override;
    ExecDataType operator==(const AbstractDataType& rightExp) const override;
    ExecDataType operator!=(const AbstractDataType& rightExp) const override;
    ExecDataType operator<(const AbstractDataType& rightExp) const override;
    ExecDataType operator>(const AbstractDataType& rightExp) const override;
    ExecDataType operator<=(const AbstractDataType& rightExp) const override;
    ExecDataType operator>=(const AbstractDataType& rightExp) const override;
    ExecDataType operator+(const AbstractDataType& rightExp) const override;
    ExecDataType operator-(const AbstractDataType& rightExp) const override;
    ExecDataType operator*(const AbstractDataType& rightExp) const override;
    ExecDataType operator/(const AbstractDataType& rightExp) const override;
    ExecDataType operator%(const AbstractDataType& rightExp) const override;
    ExecDataType operator&(const AbstractDataType& rightExp) const override;
    ExecDataType operator|(const AbstractDataType& rightExp) const override;
    ExecDataType operator^(const AbstractDataType& rightExp) const override;
    ExecDataType operator<<(const AbstractDataType& rightExp) const override;
    ExecDataType operator>>(const AbstractDataType& rightExp) const override;
    ExecDataType operator!() const override;

    template<typename CastedDataType>
    bool isType() { return std::is_same_v<ValueType, CastedDataType>; }

    template<typename CastedDataType>
    nautilus::val<CastedDataType> as() { return static_cast<nautilus::val<CastedDataType>>(rawValue); }

    // Defining friend classes so that they can access private members, and we can define operations on data types
    friend class ArithmeticalOperations;



    ~ExecutableDataType() override = default;

  protected:
    [[nodiscard]] std::string toString() const override {
        std::ostringstream oss;
        oss << " rawValue: " << rawValue << " null: " << null;
        return oss.str();
    }

  protected:
    nautilus::val<ValueType> rawValue;
};

// Define common data types
using Int8 = ExecutableDataType<int8_t>;
using Int16 = ExecutableDataType<int16_t>;
using Int32 = ExecutableDataType<int32_t>;
using Int64 = ExecutableDataType<int64_t>;
using UInt8 = ExecutableDataType<uint8_t>;
using UInt16 = ExecutableDataType<uint16_t>;
using UInt32 = ExecutableDataType<uint32_t>;
using UInt64 = ExecutableDataType<uint64_t>;
using Float = ExecutableDataType<float>;
using Double = ExecutableDataType<double>;
using Boolean = ExecutableDataType<bool>;

}// namespace NES::Nautilus

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
