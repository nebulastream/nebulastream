/**
 * UserAPIExpression class file
 * 
 * */

#include <string>
#include <sstream>

#include <CodeGen/CodeGen.hpp>
#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>
#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/UnaryOperatorStatement.hpp>

#include <API/UserAPIExpression.hpp>

namespace iotdb
{
	
	Predicate::Predicate(const BinaryOperatorType& op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right, bool bracket = false) :
		_op(op),
		_left(left),
		_right(right),
		_bracket(bracket)
	{}
	
	UserAPIExpressionPtr Predicate::copy() const{
		return std::make_shared<Predicate>(*this);
	}


	bool PredicateItem::isItem() const{
	    return true;
	}

    bool Predicate::isItem() const {
        return false;
    }

	bool PredicateItem::attributeEquals(const AttributeFieldPtr& pAttribute){
	  /* \todo: change this, use an isEqual method defined on AttributeField! */
	    return (pAttribute->toString() == _attribute->toString());
	}


    const ExpressionStatmentPtr Predicate::generateCode(GeneratedCode& code) const{
		//toDo: implement code-generation
		//BinaryOperatorStatement bin_op(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j));

		return BinaryOperatorStatement(*(_left->generateCode(code)), _op, *(_right->generateCode(code))).copy();

	}



    const ExpressionStatmentPtr PredicateItem::generateCode(GeneratedCode& code) const{
		//toDo: implement code-generation


		if(_attribute){

		    //toDo: Need an equals operator instead of true
		    if(code.struct_decl_input_tuple.getField(_attribute->name) &&
		            code.struct_decl_input_tuple.getField(_attribute->name)->getType() == _attribute->getDataType()){

                return  VarRefStatement(
                        VariableDeclaration::create(code.struct_decl_input_tuple.getField(_attribute->name)->getType(),
                                                    code.struct_decl_input_tuple.getField(_attribute->name)->getIdentifierName())
                        ).copy();
		    } // else ERROR.

		    /*
			VariableDeclaration var_decl_i =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "i", createBasicTypeValue(BasicType(INT32), "0"));
			VariableDeclaration var_decl_j =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "j", createBasicTypeValue(BasicType(INT32), "5"));
			VariableDeclaration var_decl_k =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "k", createBasicTypeValue(BasicType(INT32), "7"));
			VariableDeclaration var_decl_l =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "l", createBasicTypeValue(BasicType(INT32), "2"));

            {
                std::cout << bin_op.getCode()->code_ << std::endl;
                //CodeExpressionPtr code = bin_op.addRight(PLUS_OP, VarRefStatement(var_decl_k)).getCode();

                //std::cout << code->code_ << std::endl;
            }
            */
		}
		if(_value){
		    return ConstantExprStatement(_value).copy();
		}
	}
	
	const std::string Predicate::toString() const{
			std::stringstream stream;
			if(_bracket) stream << "(";
			stream << _left->toString() << " " << ::iotdb::toCodeExpression(_op)->code_ << " " << _right->toString() << " ";
			if(_bracket) stream << ")";
			return stream.str();
	}
	
	PredicateItem::PredicateItem(AttributeFieldPtr attribute) : 
	_mutation(PredicateItemMutation::ATTRIBUTE),
	_attribute(attribute)
	{}
	
	PredicateItem::PredicateItem(ValueTypePtr value) : 
	_mutation(PredicateItemMutation::VALUE),
	_value(value)
	{}
	
	const std::string PredicateItem::toString() const{
			switch(_mutation)
			{
				case PredicateItemMutation::ATTRIBUTE:
					return _attribute->toString();
				case PredicateItemMutation::VALUE:
					return _value->getCodeExpression()->code_;
			}
	}

	UserAPIExpressionPtr PredicateItem::copy() const{
		return std::make_shared<PredicateItem>(*this);
	}
	
	
	Predicate operator == (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator != (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::UNEQUAL_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator > (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::GREATER_THEN_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator < (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LESS_THEN_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator >= (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::GREATER_THEN_OP , lhs.copy(), rhs.copy());
	}
	Predicate operator <= (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LESS_THEN_EQUAL_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator + (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::PLUS_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator - (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::MINUS_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator * (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::MULTIPLY_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator / (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::DIVISION_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator % (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::MODULO_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator && (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LOGICAL_AND_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator || (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LOGICAL_OR_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator & (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_AND_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator | (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_OR_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator ^ (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_XOR_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator << (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_LEFT_SHIFT_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator >> (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_RIGHT_SHIFT_OP, lhs.copy(), rhs.copy());
	}
	
} //end namespace iotdb
