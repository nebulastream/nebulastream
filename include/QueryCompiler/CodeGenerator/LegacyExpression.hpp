/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef USERAPIEXPRESSION_HPP
#define USERAPIEXPRESSION_HPP

#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>

namespace NES {
namespace QueryCompilation {

enum class PredicateItemMutation { ATTRIBUTE, VALUE };

class LegacyExpression {
  public:
    virtual ~LegacyExpression(){};
    virtual const ExpressionStatmentPtr generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const = 0;
    virtual const std::string toString() const = 0;
    virtual LegacyExpressionPtr copy() const = 0;
    virtual bool equals(const LegacyExpression& rhs) const = 0;
};

class Predicate : public LegacyExpression {
  public:
    Predicate(const BinaryOperatorType& op,
              const LegacyExpressionPtr left,
              const LegacyExpressionPtr right,
              const std::string& functionCallOverload,
              bool bracket = true);
    Predicate(const BinaryOperatorType& op, const LegacyExpressionPtr left, const LegacyExpressionPtr right, bool bracket = true);

    const ExpressionStatmentPtr generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const override;
    const std::string toString() const override;
    LegacyExpressionPtr copy() const override;
    bool equals(const LegacyExpression& rhs) const override;
    BinaryOperatorType getOperatorType() const;
    const LegacyExpressionPtr getLeft() const;
    const LegacyExpressionPtr getRight() const;

  private:
    Predicate() = default;
    BinaryOperatorType op;
    LegacyExpressionPtr left;
    LegacyExpressionPtr right;
    bool bracket;
    std::string functionCallOverload;
};

class PredicateItem : public LegacyExpression {
  public:
    PredicateItem(AttributeFieldPtr attribute);
    PredicateItem(ValueTypePtr value);

    PredicateItem(int8_t val);
    PredicateItem(uint8_t val);
    PredicateItem(int16_t val);
    PredicateItem(uint16_t val);
    PredicateItem(int32_t val);
    PredicateItem(uint32_t val);
    PredicateItem(int64_t val);
    PredicateItem(uint64_t val);
    PredicateItem(float val);
    PredicateItem(double val);
    PredicateItem(bool val);
    PredicateItem(char val);
    PredicateItem(const char* val);

    const ExpressionStatmentPtr generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const override;
    const std::string toString() const override;
    LegacyExpressionPtr copy() const override;

    bool equals(const LegacyExpression& rhs) const override;

    bool isStringType() const;
    const DataTypePtr getDataTypePtr() const;
    AttributeFieldPtr getAttributeField() { return this->attribute; };
    const ValueTypePtr& getValue() const;

  private:
    PredicateItem() = default;
    PredicateItemMutation mutation;
    AttributeFieldPtr attribute = nullptr;
    ValueTypePtr value = nullptr;
};

typedef std::shared_ptr<PredicateItem> PredicateItemPtr;

class Field : public PredicateItem {
  public:
    Field(AttributeFieldPtr name);

  private:
    std::string _name;
};

typedef std::shared_ptr<Field> FieldPtr;

const PredicatePtr createPredicate(const LegacyExpression& expression);

Predicate operator==(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator!=(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator<(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator>(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator>=(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator<=(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator+(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator-(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator*(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator/(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator%(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator&&(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator||(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator&(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator|(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator^(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator<<(const LegacyExpression& lhs, const LegacyExpression& rhs);
Predicate operator>>(const LegacyExpression& lhs, const LegacyExpression& rhs);

Predicate operator==(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator!=(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator<(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator>(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator>=(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator<=(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator+(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator-(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator*(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator/(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator%(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator&&(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator||(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator&(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator|(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator^(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator<<(const PredicateItem& lhs, const LegacyExpression& rhs);
Predicate operator>>(const PredicateItem& lhs, const LegacyExpression& rhs);

Predicate operator==(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator!=(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator<(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator>(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator>=(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator<=(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator+(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator-(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator*(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator/(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator%(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator&&(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator||(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator&(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator|(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator^(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator<<(const LegacyExpression& lhs, const PredicateItem& rhs);
Predicate operator>>(const LegacyExpression& lhs, const PredicateItem& rhs);

Predicate operator==(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator!=(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator<(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator>(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator>=(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator<=(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator+(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator-(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator*(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator/(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator%(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator&&(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator||(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator&(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator|(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator^(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator<<(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator>>(const PredicateItem& lhs, const PredicateItem& rhs);
}// namespace QueryCompilation
}//end of namespace NES
#endif
