/**
 * UserAPIExpression class file
 * 
 * */

#define BRACKETMODE 1

#include <string>
#include <sstream>

#include <CodeGen/CodeExpression.hpp>

#include <API/UserAPIExpression.hpp>

namespace iotdb
{
	
	Predicate::Predicate(UserAPIBinaryExpression op, const UserAPIExpression &left, const UserAPIExpression &right) : 
		_op(op),
		_left(left),
		_right(right)
	{};
	
	Predicate Predicate::copy(){
		Predicate temp = *this;
		return temp;
	};
	
	const std::string Predicate::generateCode() const{
		std::stringstream stream;
		if(BRACKETMODE) stream << "(";
		stream << _left.generateCode() << UserAPIBinaryExpressionString.at(_op) << _right.generateCode();
		if(BRACKETMODE) stream << ")";
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
	
	PredicateItem PredicateItem::copy(){
		switch(_mutation)
		{
			case PredicateItemMutation::ATTRIBUTE:
				return PredicateItem(_attribute);
				break;
			case PredicateItemMutation::VALUE:
				return PredicateItem(_value);
				break;
		};
	};
	
	
	Predicate operator == (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(UserAPIBinaryExpression::EQUALS, lhs, rhs);
	};
	
} //end namespace iotdb
