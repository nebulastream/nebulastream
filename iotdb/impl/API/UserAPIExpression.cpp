/**
 * UserAPIExpression class file
 * 
 * */

#include <string>
#include <sstream>

#include <CodeGen/CodeExpression.hpp>

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
	
	const std::string Predicate::generateCode() const{
		//toDo: implement code-generation
		return "";
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
	
	const std::string PredicateItem::generateCode() const{
		//toDo: implement code-generation
		return "";
	};
	
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
