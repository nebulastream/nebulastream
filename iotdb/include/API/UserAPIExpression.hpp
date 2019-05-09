/**
 * UserAPIExpression header file
 * 
 * */

#ifndef USERAPIEXPRESSION_HPP
#define USERAPIEXPRESSION_HPP

#include <string>
#include <memory>

#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Core/DataTypes.hpp>

namespace iotdb
{

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

class ExpressionStatment;
typedef std::shared_ptr<ExpressionStatment> ExpressionStatmentPtr; 

enum class PredicateItemMutation{
	ATTRIBUTE,
	VALUE
};

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

	
class UserAPIExpression{
public:
	virtual ~UserAPIExpression(){};
    virtual const ExpressionStatmentPtr generateCode(GeneratedCode& code) const = 0;
    virtual const std::string toString() const = 0;
	virtual UserAPIExpressionPtr copy() const = 0;
    virtual bool isItem() const = 0;
};

class Predicate : public UserAPIExpression{
public:
	Predicate(const BinaryOperatorType& op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right, bool bracket);
	
	virtual const ExpressionStatmentPtr generateCode(GeneratedCode& code) const override;
	virtual const std::string toString() const override;
	virtual UserAPIExpressionPtr copy() const override;
    virtual bool isItem() const override;
private:
    BinaryOperatorType _op;
	const UserAPIExpressionPtr _left;
	const UserAPIExpressionPtr _right;
	bool _bracket;
};


class PredicateItem : public UserAPIExpression{
public:
	PredicateItem(AttributeFieldPtr attribute);
	PredicateItem(ValueTypePtr value);

    virtual const ExpressionStatmentPtr generateCode(GeneratedCode& code) const override;
    virtual const std::string toString() const override;
	virtual UserAPIExpressionPtr copy() const override;
	virtual bool isItem() const override;
    virtual bool attributeEquals(const AttributeFieldPtr& pAttribute);
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
