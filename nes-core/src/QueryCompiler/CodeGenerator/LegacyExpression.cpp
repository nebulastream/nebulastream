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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/TernaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/CodeGenerator/RecordHandler.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <string>
#include <utility>

namespace NES::QueryCompilation {

Predicate::Predicate(const BinaryOperatorType& op,
                     const LegacyExpressionPtr& left,
                     const LegacyExpressionPtr& right,
                     std::string functionCallOverload,
                     bool bracket)
    : op(op), left(left), right(right), bracket(bracket), functionCallOverload(std::move(functionCallOverload)) {}

Predicate::Predicate(const BinaryOperatorType& op,
                     const LegacyExpressionPtr& left,
                     const LegacyExpressionPtr& right,
                     bool bracket)
    : op(op), left(left), right(right), bracket(bracket) {}

LegacyExpressionPtr Predicate::copy() const { return std::make_shared<Predicate>(*this); }

ExpressionStatementPtr Predicate::generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const {
    if (functionCallOverload.empty()) {
        if (bracket) {
            return BinaryOperatorStatement(*(left->generateCode(code, recordHandler)),
                                           op,
                                           *(right->generateCode(code, recordHandler)),
                                           BRACKETS)
                .copy();
        }
        return BinaryOperatorStatement(*(left->generateCode(code, recordHandler)),
                                       op,
                                       *(right->generateCode(code, recordHandler)))
            .copy();
    }
    FunctionCallStatement expr = FunctionCallStatement(functionCallOverload);
    expr.addParameter(left->generateCode(code, recordHandler));
    expr.addParameter(right->generateCode(code, recordHandler));
    if (bracket) {
        return BinaryOperatorStatement(
                   expr,
                   op,
                   (ConstantExpressionStatement(NES::QueryCompilation::GeneratableTypesFactory::createValueType(
                       DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "0")))),
                   BRACKETS)
            .copy();
    }
    return BinaryOperatorStatement(expr,
                                   op,
                                   (ConstantExpressionStatement(NES::QueryCompilation::GeneratableTypesFactory::createValueType(
                                       DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), "0")))))
        .copy();
}

ExpressionStatementPtr PredicateItem::generateCode(GeneratedCodePtr&, RecordHandlerPtr recordHandler) const {
    if (attribute) {
        //checks if the predicate field is contained in the current source record.
        if (recordHandler->hasAttribute(attribute->getName())) {
            return recordHandler->getAttribute(attribute->getName());
        }
        NES_FATAL_ERROR("UserAPIExpression: Could not Retrieve Attribute from record handler!");

    } else if (value) {
        // todo remove if compiler refactored
        return ConstantExpressionStatement(NES::QueryCompilation::GeneratableTypesFactory::createValueType(value)).copy();
    }
    return nullptr;
}

std::string Predicate::toString() const {
    std::stringstream stream;
    if (bracket) {
        stream << "(";
    }
    stream << left->toString() << " " << toCodeExpression(op)->code_ << " " << right->toString() << " ";
    if (bracket) {
        stream << ")";
    }
    return stream.str();
}

bool Predicate::equals(const LegacyExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const Predicate&>(_rhs);
        if ((left == nullptr && rhs.left == nullptr) || (left->equals(*rhs.left.get()))) {
            if ((right == nullptr && rhs.right == nullptr) || (right->equals(*rhs.right.get()))) {
                return op == rhs.op && bracket == rhs.bracket && functionCallOverload == rhs.functionCallOverload;
            }
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

PredicateItem::PredicateItem(AttributeFieldPtr attribute)
    : mutation(PredicateItemMutation::ATTRIBUTE), attribute(std::move(attribute)) {}
BinaryOperatorType Predicate::getOperatorType() const { return op; }

LegacyExpressionPtr Predicate::getLeft() const { return left; }

LegacyExpressionPtr Predicate::getRight() const { return right; }

WhenPredicate::WhenPredicate(const LegacyExpressionPtr& left, const LegacyExpressionPtr& right) : left(left), right(right) {}

LegacyExpressionPtr WhenPredicate::copy() const { return std::make_shared<WhenPredicate>(*this); }

ExpressionStatementPtr WhenPredicate::generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const {
    NES_INFO("LegacyWhenPredicate: This predicate generates code, but it should never generate code on its own. The code will "
             "just return the right expression.")
    return TernaryOperatorStatement(*(left->generateCode(code, recordHandler)),
                                    *(right->generateCode(code, recordHandler)),
                                    *(right->generateCode(code, recordHandler)))
        .copy();
}

LegacyExpressionPtr WhenPredicate::getLeft() const { return left; }
LegacyExpressionPtr WhenPredicate::getRight() const { return right; }

std::string WhenPredicate::toString() const {
    std::stringstream stream;
    stream << left->toString() << " ? " << right->toString();
    return stream.str();
}

bool WhenPredicate::equals(const LegacyExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const WhenPredicate&>(_rhs);
        if ((left == nullptr && rhs.getLeft() == nullptr) || (left->equals(*rhs.getRight().get()))) {
            if ((right == nullptr && rhs.getRight() == nullptr) || (right->equals(*rhs.getRight().get()))) {
                return true;
            }
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

CasePredicate::CasePredicate(const std::vector<LegacyExpressionPtr>& whenExprs, const LegacyExpressionPtr& defautlExpr)
    : whenExprs(whenExprs), defautlExpr(defautlExpr) {}

LegacyExpressionPtr CasePredicate::copy() const { return std::make_shared<CasePredicate>(*this); }

ExpressionStatementPtr CasePredicate::generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const {
    try {
        auto lastWhen = dynamic_pointer_cast<const WhenPredicate>(*(whenExprs.end() - 1));
        //generate base expression from last when and default
        ExpressionStatementPtr curBaseExpr = TernaryOperatorStatement(*(lastWhen->getLeft()->generateCode(code, recordHandler)),
                                                                      *(lastWhen->getRight()->generateCode(code, recordHandler)),
                                                                      *(defautlExpr->generateCode(code, recordHandler)))
                                                 .copy();

        //add other when expressions in front of base expression
        for (auto whenExpIt = (whenExprs.rbegin() + 1); whenExpIt != whenExprs.rend(); ++whenExpIt) {
            auto whenExpPtr = std::dynamic_pointer_cast<const WhenPredicate>(*(whenExpIt));
            ExpressionStatementPtr tmpExpr =
                TernaryOperatorStatement(*(whenExpPtr->getLeft()->generateCode(code, recordHandler)),
                                         *(whenExpPtr->getRight()->generateCode(code, recordHandler)),
                                         *curBaseExpr)
                    .copy();
            curBaseExpr = tmpExpr;
        }
        NES_DEBUG("CasePredicate: Generating code for expression " << this->toString()
                                                                   << "\nIt looks like: " << curBaseExpr->getCode()->code_)
        return curBaseExpr;
    } catch (const std::bad_cast& e) {
        NES_FATAL_ERROR("CasePredicate: Could not cast when expressions to generate code for them.");
    }

    return nullptr;
}

std::vector<LegacyExpressionPtr> CasePredicate::getWhenExprs() const { return whenExprs; }

LegacyExpressionPtr CasePredicate::getDefaultExpr() const { return defautlExpr; }

std::string CasePredicate::toString() const {
    std::stringstream stream;
    for (auto whenExp : whenExprs) {
        stream << whenExp->toString() << " : ";
    }
    stream << defautlExpr->toString();
    return stream.str();
}

bool CasePredicate::equals(const LegacyExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const CasePredicate&>(_rhs);
        if (whenExprs.size() == rhs.getWhenExprs().size() && defautlExpr->equals(*rhs.getDefaultExpr().get())) {
            NES_DEBUG("CasePredicate: Equals in this legacy predicate only checks for same number of when cases and whether or "
                      "not the default expression is equal.")
            return true;
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

UnaryPredicate::UnaryPredicate(const UnaryOperatorType& op, const LegacyExpressionPtr& child, bool bracket)
    : op(op), child(child), bracket(bracket) {}

LegacyExpressionPtr UnaryPredicate::copy() const { return std::make_shared<UnaryPredicate>(*this); }

ExpressionStatementPtr UnaryPredicate::generateCode(GeneratedCodePtr& code, RecordHandlerPtr recordHandler) const {
    if (bracket) {
        return UnaryOperatorStatement(*(child->generateCode(code, recordHandler)), op, BRACKETS).copy();
    }
    return UnaryOperatorStatement(*(child->generateCode(code, recordHandler)), op).copy();
}

std::string UnaryPredicate::toString() const {
    std::stringstream stream;
    if (bracket) {
        stream << "(";
    }
    stream << toCodeExpression(op)->code_ << " " << child->toString();// todo this isnt right yet
    if (bracket) {
        stream << ")";
    }
    return stream.str();
}

bool UnaryPredicate::equals(const LegacyExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const UnaryPredicate&>(_rhs);
        if ((child == nullptr && rhs.child == nullptr) || (child->equals(*rhs.child.get()))) {
            return op == rhs.op && bracket == rhs.bracket;
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

UnaryOperatorType UnaryPredicate::getOperatorType() const { return op; }

PredicateItem::PredicateItem(ValueTypePtr value) : mutation(PredicateItemMutation::VALUE), value(std::move(value)) {}

PredicateItem::PredicateItem(int8_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint8_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), std::to_string(val))) {}
PredicateItem::PredicateItem(int16_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint16_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(val))) {}
PredicateItem::PredicateItem(int32_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint32_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), std::to_string(val))) {}
PredicateItem::PredicateItem(int64_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), std::to_string(val))) {}
PredicateItem::PredicateItem(uint64_t val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), std::to_string(val))) {}
PredicateItem::PredicateItem(float val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createFloat(), std::to_string(val))) {}
PredicateItem::PredicateItem(double val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createDouble(), std::to_string(val))) {}
PredicateItem::PredicateItem(bool val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createBoolean(), std::to_string(val))) {}
PredicateItem::PredicateItem(char val)
    : mutation(PredicateItemMutation::VALUE),
      value(DataTypeFactory::createBasicValue(DataTypeFactory::createChar(), std::to_string(val))) {}
PredicateItem::PredicateItem(const char* val)
    : mutation(PredicateItemMutation::VALUE), value(DataTypeFactory::createFixedCharValue(val)) {}

std::string PredicateItem::toString() const {
    switch (mutation) {
        case PredicateItemMutation::ATTRIBUTE: return attribute->toString();
        case PredicateItemMutation::VALUE: {
            return NES::QueryCompilation::GeneratableTypesFactory::createValueType(value)->getCodeExpression()->code_;
        }
    }
    return "";
}

DataTypePtr PredicateItem::getDataTypePtr() const {
    if (attribute) {
        return attribute->getDataType();
    }
    return value->dataType;
};

LegacyExpressionPtr PredicateItem::copy() const { return std::make_shared<PredicateItem>(*this); }

bool PredicateItem::equals(const LegacyExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const PredicateItem&>(_rhs);
        if ((attribute == nullptr && rhs.attribute == nullptr) || attribute->isEqual(rhs.attribute)) {
            if ((value == nullptr && rhs.value == nullptr) || (value->isEquals(rhs.value))) {
                return mutation == rhs.mutation;
            }
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

const ValueTypePtr& PredicateItem::getValue() const { return value; }

Field::Field(const AttributeFieldPtr& field) : PredicateItem(field), _name(field->getName()) {}

PredicatePtr createPredicate(const LegacyExpression& expression) {
    PredicatePtr value = std::dynamic_pointer_cast<Predicate>(expression.copy());
    if (!value) {
        NES_ERROR("UserAPIExpression is not a predicate");
    }
    return value;
}

Predicate operator==(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator!=(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::UNEQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator>(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::GREATER_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator<(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LESS_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator>=(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::GREATER_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator<=(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LESS_THAN_EQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator+(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::PLUS_OP, lhs.copy(), rhs.copy());
}
Predicate operator-(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::MINUS_OP, lhs.copy(), rhs.copy());
}
Predicate operator*(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::MULTIPLY_OP, lhs.copy(), rhs.copy());
}
Predicate operator/(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::DIVISION_OP, lhs.copy(), rhs.copy());
}
Predicate operator%(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::MODULO_OP, lhs.copy(), rhs.copy());
}
Predicate operator&&(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LOGICAL_AND_OP, lhs.copy(), rhs.copy());
}
Predicate operator||(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::LOGICAL_OR_OP, lhs.copy(), rhs.copy());
}
Predicate operator&(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_AND_OP, lhs.copy(), rhs.copy());
}
Predicate operator|(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_OR_OP, lhs.copy(), rhs.copy());
}
Predicate operator^(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_XOR_OP, lhs.copy(), rhs.copy());
}
Predicate operator<<(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_LEFT_SHIFT_OP, lhs.copy(), rhs.copy());
}
Predicate operator>>(const LegacyExpression& lhs, const LegacyExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_RIGHT_SHIFT_OP, lhs.copy(), rhs.copy());
}

Predicate operator==(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return (lhs == dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator!=(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator!=(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator>(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator<(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>=(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator>=(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<=(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator<=(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator+(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator+(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator-(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator-(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator*(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator*(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator/(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator/(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator%(const LegacyExpression& lhs, const PredicateItem& rhs) {
    if (!rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator%(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator&&(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator&&(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator||(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator||(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator|(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator|(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator^(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator^(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<<(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator<<(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>>(const LegacyExpression& lhs, const PredicateItem& rhs) {
    return operator>>(lhs, dynamic_cast<const LegacyExpression&>(rhs));
}

Predicate operator==(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return (dynamic_cast<const LegacyExpression&>(lhs) == rhs);
}
Predicate operator!=(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator!=(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator>(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator>(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator<(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator<(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator>=(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator>=(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator<=(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator<=(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator+(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator+(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator-(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator-(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator*(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator*(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator/(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator/(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator%(const PredicateItem& lhs, const LegacyExpression& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator%(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator&&(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator&&(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator||(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator||(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator|(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator|(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator^(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator^(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator<<(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator<<(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}
Predicate operator>>(const PredicateItem& lhs, const LegacyExpression& rhs) {
    return operator>>(dynamic_cast<const LegacyExpression&>(lhs), rhs);
}

/**
 * Operator overload includes String compare by define a function-call-overload in the code generation process
 * @param lhs
 * @param rhs
 * @return
 */
Predicate operator==(const PredicateItem& lhs, const PredicateItem& rhs) {
    return (dynamic_cast<LegacyExpression const&>(lhs) == dynamic_cast<LegacyExpression const&>(rhs));
}
Predicate operator!=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator!=(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>=(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<=(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator+(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator+(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator-(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator-(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator*(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator*(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator/(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator/(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator%(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (!lhs.getDataTypePtr()->isNumeric() || !rhs.getDataTypePtr()->isNumeric()) {
        NES_ERROR("NOT A NUMERICAL VALUE");
    }
    return operator%(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator&&(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator&&(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator||(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator||(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator|(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator|(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator^(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator^(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator<<(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<<(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
Predicate operator>>(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>>(dynamic_cast<const LegacyExpression&>(lhs), dynamic_cast<const LegacyExpression&>(rhs));
}
}// namespace NES::QueryCompilation
