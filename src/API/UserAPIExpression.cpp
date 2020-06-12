#include <sstream>
#include <string>

#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedCode.hpp>

#include <API/Types/DataTypes.hpp>
#include <API/UserAPIExpression.hpp>
#include <Util/Logger.hpp>
namespace NES {

Predicate::Predicate(const BinaryOperatorType& op,
                     const UserAPIExpressionPtr left,
                     const UserAPIExpressionPtr right,
                     const std::string& functionCallOverload,
                     bool bracket) : op(op),
                                     left(left),
                                     right(right),
                                     bracket(bracket),
                                     functionCallOverload(functionCallOverload) {}

Predicate::Predicate(const BinaryOperatorType& op,
                     const UserAPIExpressionPtr left,
                     const UserAPIExpressionPtr right,
                     bool bracket) : op(op),
                                     left(left),
                                     right(right),
                                     bracket(bracket),
                                     functionCallOverload("") {}

UserAPIExpressionPtr Predicate::copy() const {
    return std::make_shared<Predicate>(*this);
}

const ExpressionStatmentPtr Predicate::generateCode(GeneratedCodePtr& code) const {
    if (functionCallOverload.empty()) {
        if (bracket)
            return BinaryOperatorStatement(*(left->generateCode(code)),
                                           op,
                                           *(right->generateCode(code)),
                                           BRACKETS)
                .copy();
        return BinaryOperatorStatement(*(left->generateCode(code)), op, *(right->generateCode(code))).copy();
    } else {
        std::stringstream str;
        FunctionCallStatement expr = FunctionCallStatement(functionCallOverload);
        expr.addParameter(left->generateCode(code));
        expr.addParameter(right->generateCode(code));
        if (bracket)
            return BinaryOperatorStatement(expr,
                                           op,
                                           (ConstantExpressionStatement((createBasicTypeValue(BasicType::UINT8, "0")))),
                                           BRACKETS)
                .copy();
        return BinaryOperatorStatement(expr,
                                       op,
                                       (ConstantExpressionStatement((createBasicTypeValue(BasicType::UINT8, "0")))))
            .copy();
    }
}

const ExpressionStatmentPtr PredicateItem::generateCode(GeneratedCodePtr& code) const {
    if (attribute) {
        //checks if the predicate field is contained in the struct declaration.
        if (code->structDeclaratonInputTuple.containsField(attribute->name, attribute->getDataType())) {
            // creates a new variable declaration for the predicate field.
            VariableDeclaration var_decl_attr = code->structDeclaratonInputTuple.getVariableDeclaration(attribute->name);
            return ((VarRef(code->varDeclarationInputTuples)[VarRef(*code->varDeclarationRecordIndex)]).accessRef(VarRef(var_decl_attr))).copy();
        } else {
            NES_FATAL_ERROR("UserAPIExpression: Could not Retrieve Attribute from StructDeclaration!");
        }
    } else if (value) {
        return ConstantExpressionStatement(value).copy();
    } else {
        NES_FATAL_ERROR("UserAPIExpression: PredicateItem has only NULL Pointers!");
    }
}

const std::string Predicate::toString() const {
    std::stringstream stream;
    if (bracket)
        stream << "(";
    stream << left->toString() << " " << ::NES::toCodeExpression(op)->code_ << " " << right->toString() << " ";
    if (bracket)
        stream << ")";
    return stream.str();
}

bool Predicate::equals(const UserAPIExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const NES::Predicate&>(_rhs);
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

PredicateItem::PredicateItem(AttributeFieldPtr attribute) : mutation(PredicateItemMutation::ATTRIBUTE),
                                                            attribute(attribute) {}
BinaryOperatorType Predicate::getOperatorType() const {
    return op;
}

const UserAPIExpressionPtr Predicate::getLeft() const {
    return left;
}

const UserAPIExpressionPtr Predicate::getRight() const {
    return right;
}

PredicateItem::PredicateItem(ValueTypePtr value) : mutation(PredicateItemMutation::VALUE),
                                                   value(value) {}

PredicateItem::PredicateItem(int8_t val) : mutation(PredicateItemMutation::VALUE),
                                           value(createBasicTypeValue(BasicType::INT8, std::to_string(val))) {}
PredicateItem::PredicateItem(uint8_t val) : mutation(PredicateItemMutation::VALUE),
                                            value(createBasicTypeValue(BasicType::UINT8, std::to_string(val))) {}
PredicateItem::PredicateItem(int16_t val) : mutation(PredicateItemMutation::VALUE),
                                            value(createBasicTypeValue(BasicType::INT16, std::to_string(val))) {}
PredicateItem::PredicateItem(uint16_t val) : mutation(PredicateItemMutation::VALUE),
                                             value(createBasicTypeValue(BasicType::UINT16, std::to_string(val))) {}
PredicateItem::PredicateItem(int32_t val) : mutation(PredicateItemMutation::VALUE),
                                            value(createBasicTypeValue(BasicType::INT32, std::to_string(val))) {}
PredicateItem::PredicateItem(uint32_t val) : mutation(PredicateItemMutation::VALUE),
                                             value(createBasicTypeValue(BasicType::UINT32, std::to_string(val))) {}
PredicateItem::PredicateItem(int64_t val) : mutation(PredicateItemMutation::VALUE),
                                            value(createBasicTypeValue(BasicType::INT64, std::to_string(val))) {}
PredicateItem::PredicateItem(uint64_t val) : mutation(PredicateItemMutation::VALUE),
                                             value(createBasicTypeValue(BasicType::UINT64, std::to_string(val))) {}
PredicateItem::PredicateItem(float val) : mutation(PredicateItemMutation::VALUE),
                                          value(createBasicTypeValue(BasicType::FLOAT32, std::to_string(val))) {}
PredicateItem::PredicateItem(double val) : mutation(PredicateItemMutation::VALUE),
                                           value(createBasicTypeValue(BasicType::FLOAT64, std::to_string(val))) {}
PredicateItem::PredicateItem(bool val) : mutation(PredicateItemMutation::VALUE),
                                         value(createBasicTypeValue(BasicType::BOOLEAN, std::to_string(val))) {}
PredicateItem::PredicateItem(char val) : mutation(PredicateItemMutation::VALUE),
                                         value(createBasicTypeValue(BasicType::CHAR, std::to_string(val))) {}
PredicateItem::PredicateItem(const char* val) : mutation(PredicateItemMutation::VALUE),
                                                value(createStringValueType(val)) {}

const std::string PredicateItem::toString() const {
    switch (mutation) {
        case PredicateItemMutation::ATTRIBUTE: return attribute->toString();
        case PredicateItemMutation::VALUE: return value->getCodeExpression()->code_;
    }
    return "";
}

bool PredicateItem::isStringType() const {
    return (getDataTypePtr()->isCharDataType()) && (getDataTypePtr()->isArrayDataType());
}

const DataTypePtr PredicateItem::getDataTypePtr() const {
    if (attribute)
        return attribute->getDataType();
    return value->getType();
};

UserAPIExpressionPtr PredicateItem::copy() const {
    return std::make_shared<PredicateItem>(*this);
}

bool PredicateItem::equals(const UserAPIExpression& _rhs) const {
    try {
        auto rhs = dynamic_cast<const NES::PredicateItem&>(_rhs);
        if ((attribute == nullptr && rhs.attribute == nullptr) || (*attribute.get() == *rhs.attribute.get())) {
            if ((value == nullptr && rhs.value == nullptr) || (*value.get() == *rhs.value.get())) {
                return mutation == rhs.mutation;
            }
        }
        return false;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

const ValueTypePtr& PredicateItem::getValue() const {
    return value;
}

Field::Field(AttributeFieldPtr field) : PredicateItem(field), _name(field->name) {}

const PredicatePtr createPredicate(const UserAPIExpression& expression) {
    PredicatePtr value = std::dynamic_pointer_cast<Predicate>(expression.copy());
    if (!value) {
        NES_ERROR("UserAPIExpression is not a predicate");
    }
    return value;
}

Predicate operator==(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator!=(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::UNEQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator>(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::GREATER_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator<(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::LESS_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator>=(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::GREATER_THAN_OP, lhs.copy(), rhs.copy());
}
Predicate operator<=(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::LESS_THAN_EQUAL_OP, lhs.copy(), rhs.copy());
}
Predicate operator+(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::PLUS_OP, lhs.copy(), rhs.copy());
}
Predicate operator-(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::MINUS_OP, lhs.copy(), rhs.copy());
}
Predicate operator*(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::MULTIPLY_OP, lhs.copy(), rhs.copy());
}
Predicate operator/(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::DIVISION_OP, lhs.copy(), rhs.copy());
}
Predicate operator%(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::MODULO_OP, lhs.copy(), rhs.copy());
}
Predicate operator&&(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::LOGICAL_AND_OP, lhs.copy(), rhs.copy());
}
Predicate operator||(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::LOGICAL_OR_OP, lhs.copy(), rhs.copy());
}
Predicate operator&(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_AND_OP, lhs.copy(), rhs.copy());
}
Predicate operator|(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_OR_OP, lhs.copy(), rhs.copy());
}
Predicate operator^(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_XOR_OP, lhs.copy(), rhs.copy());
}
Predicate operator<<(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_LEFT_SHIFT_OP, lhs.copy(), rhs.copy());
}
Predicate operator>>(const UserAPIExpression& lhs, const UserAPIExpression& rhs) {
    return Predicate(BinaryOperatorType::BITWISE_RIGHT_SHIFT_OP, lhs.copy(), rhs.copy());
}

Predicate operator==(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return (lhs == dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator!=(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator!=(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator>(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator>(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator<(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator<(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator>=(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator>=(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator<=(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator<=(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator+(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    if (rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator+(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator-(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    if (rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator-(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator*(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    if (rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator*(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator/(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    if (rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator/(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator%(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    if (rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator%(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator&&(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator&&(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator||(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator||(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator|(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator|(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator^(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator^(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator<<(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator<<(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator>>(const UserAPIExpression& lhs, const PredicateItem& rhs) {
    return operator>>(lhs, dynamic_cast<const UserAPIExpression&>(rhs));
}

Predicate operator==(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return (dynamic_cast<const UserAPIExpression&>(lhs) == rhs);
}
Predicate operator!=(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator!=(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator>(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator>(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator<(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator<(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator>=(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator>=(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator<=(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator<=(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator+(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator+(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator-(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator-(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator*(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator*(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator/(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator/(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator%(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator%(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator&&(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator&&(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator||(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator||(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator|(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator|(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator^(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator^(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator<<(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator<<(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}
Predicate operator>>(const PredicateItem& lhs, const UserAPIExpression& rhs) {
    return operator>>(dynamic_cast<const UserAPIExpression&>(lhs), rhs);
}

/**
 * Operator overload includes String compare by define a function-call-overload in the code generation process
 * @param lhs
 * @param rhs
 * @return
 */
Predicate operator==(const PredicateItem& lhs, const PredicateItem& rhs) {
    //possible use of memcmp when arraytypes equal with length is equal...
    int checktype = lhs.isStringType();
    checktype += rhs.isStringType();
    if (checktype == 1)
        NES_ERROR("NOT COMPARABLE TYPES");
    if (checktype == 2)
        return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), rhs.copy(), "strcmp", false);
    return (dynamic_cast<const UserAPIExpression&>(lhs) == dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator!=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator!=(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator>(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator<(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator>=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>=(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator<=(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<=(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator+(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR))
        || rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator+(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator-(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR))
        || rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator-(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator*(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR))
        || rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator*(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator/(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR))
        || rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator/(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator%(const PredicateItem& lhs, const PredicateItem& rhs) {
    if (lhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR))
        || rhs.getDataTypePtr()->isEqual(createDataType(BasicType::CHAR)))
        NES_ERROR("NOT A NUMERICAL VALUE");
    return operator%(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator&&(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator&&(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator||(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator||(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator|(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator|(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator^(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator^(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator<<(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator<<(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
Predicate operator>>(const PredicateItem& lhs, const PredicateItem& rhs) {
    return operator>>(dynamic_cast<const UserAPIExpression&>(lhs), dynamic_cast<const UserAPIExpression&>(rhs));
}
}//end namespace NES
