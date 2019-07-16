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
};

class Predicate : public UserAPIExpression{
public:
	Predicate(const BinaryOperatorType& op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right, const std::string& functionCallOverload, bool bracket = false);
    Predicate(const BinaryOperatorType& op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right, bool bracket = false);

	virtual const ExpressionStatmentPtr generateCode(GeneratedCode& code) const override;
	virtual const std::string toString() const override;
	virtual UserAPIExpressionPtr copy() const override;
private:
    BinaryOperatorType _op;
	const UserAPIExpressionPtr _left;
	const UserAPIExpressionPtr _right;
	bool _bracket;
	const std::string _functionCallOverload;
};


class PredicateItem : public UserAPIExpression{
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

    virtual const ExpressionStatmentPtr generateCode(GeneratedCode& code) const override;
    virtual const std::string toString() const override;
	virtual UserAPIExpressionPtr copy() const override;

    const bool isStringType() const;
    const DataTypePtr getDataTypePtr() const;
private:
	PredicateItemMutation _mutation;
	AttributeFieldPtr _attribute=nullptr;
	ValueTypePtr _value=nullptr;
};

class Field : public PredicateItem{
    public:
        Field(std::string name);

    std::string _name;
};

Predicate operator == (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator != (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator < (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator > (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator >= (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator <= (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator + (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator - (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator * (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator / (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator % (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator && (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator || (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator & (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator | (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator ^ (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator << (const UserAPIExpression &lhs, const UserAPIExpression &rhs);
Predicate operator >> (const UserAPIExpression &lhs, const UserAPIExpression &rhs);


Predicate operator == (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator != (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator < (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator > (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator >= (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator <= (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator + (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator - (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator * (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator / (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator % (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator && (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator || (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator & (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator | (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator ^ (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator << (const PredicateItem &lhs, const UserAPIExpression &rhs);
Predicate operator >> (const PredicateItem &lhs, const UserAPIExpression &rhs);


Predicate operator == (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator != (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator < (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator > (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator >= (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator <= (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator + (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator - (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator * (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator / (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator % (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator && (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator || (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator & (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator | (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator ^ (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator << (const UserAPIExpression &lhs, const PredicateItem &rhs);
Predicate operator >> (const UserAPIExpression &lhs, const PredicateItem &rhs);


Predicate operator == (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator != (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator < (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator > (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator >= (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator <= (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator + (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator - (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator * (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator / (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator % (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator && (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator || (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator & (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator | (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator ^ (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator << (const PredicateItem &lhs, const PredicateItem &rhs);
Predicate operator >> (const PredicateItem &lhs, const PredicateItem &rhs);

} //end of namespace iotdb
#endif 
