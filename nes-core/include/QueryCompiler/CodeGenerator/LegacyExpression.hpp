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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_LEGACY_EXPRESSION_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_LEGACY_EXPRESSION_HPP_

#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>

namespace NES {
namespace QueryCompilation {

enum class PredicateItemMutation { ATTRIBUTE, VALUE };

class LegacyExpression {
  public:
    virtual ~LegacyExpression() = default;
    ;
    virtual ExpressionStatementPtr generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const = 0;
    [[nodiscard]] virtual std::string toString() const = 0;
    [[nodiscard]] virtual LegacyExpressionPtr copy() const = 0;
    [[nodiscard]] virtual bool equals(const LegacyExpression& rhs) const = 0;
};

class Predicate : public LegacyExpression {
  public:
    Predicate(BinaryOperatorType const& op,
              LegacyExpressionPtr const& left,
              LegacyExpressionPtr const& right,
              std::string functionCallOverload,
              bool bracket = true);

    Predicate(BinaryOperatorType const& op,
              LegacyExpressionPtr const& left,
              LegacyExpressionPtr const& right,
              bool bracket = true);

    ExpressionStatementPtr generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const override;
    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] LegacyExpressionPtr copy() const override;
    [[nodiscard]] bool equals(const LegacyExpression& rhs) const override;
    [[nodiscard]] BinaryOperatorType getOperatorType() const;
    [[nodiscard]] LegacyExpressionPtr getLeft() const;
    [[nodiscard]] LegacyExpressionPtr getRight() const;

  private:
    Predicate() = default;
    BinaryOperatorType op;
    LegacyExpressionPtr left;
    LegacyExpressionPtr right;
    bool bracket{};
    std::string functionCallOverload;
};

class UnaryPredicate : public LegacyExpression {
  public:
    UnaryPredicate(UnaryOperatorType const& op, LegacyExpressionPtr const& child, bool bracket = true);

    ExpressionStatementPtr generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const override;
    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] LegacyExpressionPtr copy() const override;
    [[nodiscard]] bool equals(const LegacyExpression& rhs) const override;
    [[nodiscard]] UnaryOperatorType getOperatorType() const;
    [[nodiscard]] LegacyExpressionPtr getChild() const;

  private:
    UnaryPredicate() = default;
    UnaryOperatorType op;
    LegacyExpressionPtr child;
    bool bracket{};
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

    ExpressionStatementPtr generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const override;
    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] LegacyExpressionPtr copy() const override;

    [[nodiscard]] bool equals(const LegacyExpression& rhs) const override;

    [[nodiscard]] bool isStringType() const;
    [[nodiscard]] DataTypePtr getDataTypePtr() const;
    AttributeFieldPtr getAttributeField() { return this->attribute; };
    [[nodiscard]] const ValueTypePtr& getValue() const;

  private:
    PredicateItem() = default;
    PredicateItemMutation mutation;
    AttributeFieldPtr attribute = nullptr;
    ValueTypePtr value = nullptr;
};

using PredicateItemPtr = std::shared_ptr<PredicateItem>;

class Field : public PredicateItem {
  public:
    Field(const AttributeFieldPtr& field);

  private:
    std::string _name;
};

using FieldPtr = std::shared_ptr<Field>;

PredicatePtr createPredicate(const LegacyExpression& expression);

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
#endif// NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_LEGACY_EXPRESSION_HPP_
