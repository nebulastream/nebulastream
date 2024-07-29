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

class AbstractDataType {
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

    // Defining operations on data types with other ExecDataTypes
    virtual ExecDataType operator&&(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator||(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator==(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator!=(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator<(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator>(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator<=(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator>=(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator+(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator-(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator*(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator/(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator%(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator&(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator|(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator^(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator<<(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator>>(const ExecDataType& rightExp) const { NES_NOT_IMPLEMENTED(); };
    virtual ExecDataType operator!() const { NES_NOT_IMPLEMENTED(); };

    template<typename>
    bool isType() {
        return false;
    };

    template<typename CastedDataType>
    nautilus::val<CastedDataType> as() const {
        NES_NOT_IMPLEMENTED();
    }

  protected:
    [[nodiscard]] virtual std::string toString() const = 0;

    nautilus::val<bool> null;
};

}// namespace NES::Nautilus

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_ABSTRACTDATATYPE_HPP
