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

#ifndef VARIABLESIZEEXECUTABLEDATATYPE_HPP
#define VARIABLESIZEEXECUTABLEDATATYPE_HPP

#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <iomanip>

namespace NES::Nautilus {

/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
class VariableSizeExecutableDataType : public AbstractDataType {
  public:
    VariableSizeExecutableDataType(const MemRefVal& content,
                               const nautilus::val<uint32_t>& size,
                               const nautilus::val<bool>& null)
        : AbstractDataType(null), size(size), content(content) {}

    static ExecDataType create(const MemRefVal& content, const nautilus::val<uint32_t>& size, const bool null = false) {
        return std::make_shared<VariableSizeExecutableDataType>(content, size, null);
    }

    static ExecDataType create(const MemRefVal& pointerToVarSized, const bool null = false) {
        const auto varSized = readValueFromMemRef(pointerToVarSized, uint32_t);
        return create(pointerToVarSized, varSized, null);
    }

    ~VariableSizeExecutableDataType() override = default;

    [[nodiscard]] nautilus::val<uint32_t> getSize() const { return size; }
    [[nodiscard]] MemRefVal getContent() const { return content + UInt64Val(sizeof(uint32_t)); }
    [[nodiscard]] MemRefVal getReference() const { return content ; }

  protected:
    ExecDataType operator&&(const ExecDataType&) const override;
    ExecDataType operator||(const ExecDataType&) const override;
    ExecDataType operator==(const ExecDataType&) const override;
    ExecDataType operator!=(const ExecDataType&) const override;
    ExecDataType operator<(const ExecDataType&) const override;
    ExecDataType operator>(const ExecDataType&) const override;
    ExecDataType operator<=(const ExecDataType&) const override;
    ExecDataType operator>=(const ExecDataType&) const override;
    ExecDataType operator+(const ExecDataType&) const override;
    ExecDataType operator-(const ExecDataType&) const override;
    ExecDataType operator*(const ExecDataType&) const override;
    ExecDataType operator/(const ExecDataType&) const override;
    ExecDataType operator%(const ExecDataType&) const override;
    ExecDataType operator&(const ExecDataType&) const override;
    ExecDataType operator|(const ExecDataType&) const override;
    ExecDataType operator^(const ExecDataType&) const override;
    ExecDataType operator<<(const ExecDataType&) const override;
    ExecDataType operator>>(const ExecDataType&) const override;
    ExecDataType operator!() const override;

    [[nodiscard]] std::string toString() const override {
        if (null) {
            return "NULL";
        } else {
            std::stringstream ss;
            ss << "Size(" << size.toString() << ")";
            /// Once https://github.com/nebulastream/nautilus/pull/27 is merged, we can print out the content via hex
            return ss.str();
        }
    }

    nautilus::val<bool> isBool() const override {
        return size > 0 && content != nullptr;
    }

    const nautilus::val<uint32_t> size;
    const MemRefVal content;
};
}

#endif //VARIABLESIZEEXECUTABLEDATATYPE_HPP
