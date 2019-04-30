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
	BIGGERTHEN,
	SMALLERTHEN
};

const boost::unordered_map<UserAPIBinaryExpression, std::string> UserAPIBinaryExpressionString = boost::assign::map_list_of
	(UserAPIBinaryExpression::EQUALS, 		" == ")
	(UserAPIBinaryExpression::BIGGERTHEN, 	" > ")
	(UserAPIBinaryExpression::SMALLERTHEN, 	" < ");
	
class UserAPIExpression{
public:
	virtual ~UserAPIExpression(){};
	virtual const std::string generateCode() const=0;
};

typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;


class Predicate : public UserAPIExpression{
public:
	Predicate(UserAPIBinaryExpression op, const UserAPIExpression &left, const UserAPIExpression &right);
	
	virtual const std::string generateCode() const override;
	Predicate copy();
private:
	UserAPIBinaryExpression _op;
	const UserAPIExpression &_left;
	const UserAPIExpression &_right;
};


class PredicateItem : public UserAPIExpression{
public:
	PredicateItem(AttributeFieldPtr attribute);
	PredicateItem(ValueTypePtr value);
	
	virtual const std::string generateCode() const override;
	PredicateItem copy();
private:
	PredicateItemMutation _mutation;
	AttributeFieldPtr _attribute=nullptr;
	ValueTypePtr _value=nullptr;
};

Predicate operator == (const UserAPIExpression &rhs, const UserAPIExpression &lhs);


} //end of namespace iotdb
#endif 
