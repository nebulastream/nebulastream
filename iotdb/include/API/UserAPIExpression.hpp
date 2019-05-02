/**
 * UserAPIExpression header file
 * 
 * */

#ifndef USERAPIEXPRESSION_HPP
#define USERAPIEXPRESSION_HPP

#include <string>
#include <memory>

#include <Core/DataTypes.hpp>

#include <boost/unordered_map.hpp>
#include <boost/assign/list_of.hpp>

namespace iotdb
{

enum class PredicateItemMutation{
	ATTRIBUTE,
	VALUE
};

enum class UserAPIBinaryExpression{
	EQUALS,
	NOTEQUALS,
	BIGGEREQUALS,
	SMALLEREQUALS,
	BIGGER,
	SMALLER,
	ADD,
	SUB,
	MUL,
	DIV,
	MOD,
	LAZYAND,
	LAZYOR,
	AND,
	OR,
	XOR,
	LEFTSHIFT,
	RIGHTSHIFT
};

const boost::unordered_map<UserAPIBinaryExpression, std::string> UserAPIBinaryExpressionString = boost::assign::map_list_of
	(UserAPIBinaryExpression::EQUALS, 			" == ")
	(UserAPIBinaryExpression::NOTEQUALS,		" != ")
	(UserAPIBinaryExpression::BIGGEREQUALS,		" >= ")
	(UserAPIBinaryExpression::SMALLEREQUALS,	" <= ")
	(UserAPIBinaryExpression::BIGGER,			" > ")
	(UserAPIBinaryExpression::SMALLER,			" < ")
	(UserAPIBinaryExpression::ADD,				" + ")
	(UserAPIBinaryExpression::SUB,				" - ")
	(UserAPIBinaryExpression::MUL,				" * ")
	(UserAPIBinaryExpression::DIV,				" / ")
	(UserAPIBinaryExpression::MOD,				" % ")
	(UserAPIBinaryExpression::LAZYAND,			" && ")
	(UserAPIBinaryExpression::LAZYOR,			" || ")
	(UserAPIBinaryExpression::OR,				" | ")
	(UserAPIBinaryExpression::AND,				" & ")
	(UserAPIBinaryExpression::XOR,				" ^ ")
	(UserAPIBinaryExpression::LEFTSHIFT,		" << ")
	(UserAPIBinaryExpression::RIGHTSHIFT,		" >> ");
	

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

	
class UserAPIExpression{
public:
	virtual ~UserAPIExpression(){};
	virtual const std::string generateCode() const = 0;
	virtual const std::string toString() const = 0;
	virtual UserAPIExpressionPtr copy() const = 0;
};

class Predicate : public UserAPIExpression{
public:
	Predicate(UserAPIBinaryExpression op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right, bool bracket);
	
	virtual const std::string generateCode() const override;
	virtual const std::string toString() const override;
	virtual UserAPIExpressionPtr copy() const override;
private:
	UserAPIBinaryExpression _op;
	const UserAPIExpressionPtr _left;
	const UserAPIExpressionPtr _right;
	bool _bracket;
};


class PredicateItem : public UserAPIExpression{
public:
	PredicateItem(AttributeFieldPtr attribute);
	PredicateItem(ValueTypePtr value);
	
	virtual const std::string generateCode() const override;
	virtual const std::string toString() const override;
	virtual UserAPIExpressionPtr copy() const override;
private:
	PredicateItemMutation _mutation;
	AttributeFieldPtr _attribute=nullptr;
	ValueTypePtr _value=nullptr;
};

Predicate operator == (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator != (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator < (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator > (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator >= (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator <= (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator + (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator - (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator * (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator / (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator % (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator && (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator || (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator & (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator | (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator ^ (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator << (const UserAPIExpression &rhs, const UserAPIExpression &lhs);
Predicate operator >> (const UserAPIExpression &rhs, const UserAPIExpression &lhs);


} //end of namespace iotdb
#endif 
