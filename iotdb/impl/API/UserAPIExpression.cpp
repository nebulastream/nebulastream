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
	
	Predicate::Predicate(UserAPIBinaryExpression op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right, bool bracket = true) :
		_op(op),
		_left(left),
		_right(right),
		_bracket(bracket)
	{};
	
	UserAPIExpressionPtr Predicate::copy() const{
		return std::make_shared<Predicate>(*this);
	};
	
	bool Predicate::generateCode(GeneratedCode& code) const{
		//toDo: implement code-generation
		return true;
	};
	
	bool PredicateItem::generateCode(GeneratedCode& code) const{
		//toDo: implement code-generation
		
		
		
		code_.
		if(_attribute){
			
			
			VariableDeclaration var_decl_i =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "i", createBasicTypeValue(BasicType(INT32), "0"));
			VariableDeclaration var_decl_j =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "j", createBasicTypeValue(BasicType(INT32), "5"));
			VariableDeclaration var_decl_k =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "k", createBasicTypeValue(BasicType(INT32), "7"));
			VariableDeclaration var_decl_l =
				VariableDeclaration::create(createDataType(BasicType(INT32)), "l", createBasicTypeValue(BasicType(INT32), "2"));

    {
        BinaryOperatorStatement bin_op(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j));
        std::cout << bin_op.getCode()->code_ << std::endl;
        CodeExpressionPtr code = bin_op.addRight(PLUS_OP, VarRefStatement(var_decl_k)).getCode();

        std::cout << code->code_ << std::endl;
    }
		}
		 
		
		BinaryOperatorStatement()
		
		return true;
	};
	
	const std::string Predicate::toString() const{
			std::stringstream stream;
			if(_bracket) stream << "(";
			stream << _left->toString() << UserAPIBinaryExpressionString.at(_op) << _right->toString();
			if(_bracket) stream << ")";
			return stream.str();
		};
	
	PredicateItem::PredicateItem(AttributeFieldPtr attribute) : 
	_mutation(PredicateItemMutation::ATTRIBUTE),
	_attribute(attribute)
	{};
	
	PredicateItem::PredicateItem(ValueTypePtr value) : 
	_mutation(PredicateItemMutation::VALUE),
	_value(value)
	{};
	
	const std::string PredicateItem::toString() const{
			switch(_mutation)
			{
				case PredicateItemMutation::ATTRIBUTE:
					return _attribute->toString();
					break;
				case PredicateItemMutation::VALUE:
					return _value->getCodeExpression()->code_;
					break;
			};
			return "";
		};

	UserAPIExpressionPtr PredicateItem::copy() const{
		return std::make_shared<PredicateItem>(*this);
	};
	
	
	Predicate operator == (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::EQUALS, lhs.copy(), rhs.copy());
	};
	Predicate operator != (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::NOTEQUALS, lhs.copy(), rhs.copy());
	};
	Predicate operator > (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::BIGGER, lhs.copy(), rhs.copy());
	};
	Predicate operator < (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::SMALLER, lhs.copy(), rhs.copy());
	};
	Predicate operator >= (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::BIGGEREQUALS, lhs.copy(), rhs.copy());
	};
	Predicate operator <= (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::SMALLEREQUALS, lhs.copy(), rhs.copy());
	};
	Predicate operator + (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::ADD, lhs.copy(), rhs.copy());
	};
	Predicate operator - (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::SUB, lhs.copy(), rhs.copy());
	};
	Predicate operator * (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::MUL, lhs.copy(), rhs.copy());
	};
	Predicate operator / (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::DIV, lhs.copy(), rhs.copy());
	};
	Predicate operator % (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::MOD, lhs.copy(), rhs.copy());
	};
	Predicate operator && (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::LAZYAND, lhs.copy(), rhs.copy());
	};
	Predicate operator || (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::LAZYOR, lhs.copy(), rhs.copy());
	};
	Predicate operator & (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::AND, lhs.copy(), rhs.copy());
	};
	Predicate operator | (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::OR, lhs.copy(), rhs.copy());
	};
	Predicate operator ^ (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::XOR, lhs.copy(), rhs.copy());
	};
	Predicate operator << (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::LEFTSHIFT, lhs.copy(), rhs.copy());
	};
	Predicate operator >> (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::RIGHTSHIFT, lhs.copy(), rhs.copy());
	};
	
} //end namespace iotdb
