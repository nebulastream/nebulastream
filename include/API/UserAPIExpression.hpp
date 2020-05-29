/**
 * UserAPIExpression header file
 * 
 * */

#ifndef USERAPIEXPRESSION_HPP
#define USERAPIEXPRESSION_HPP

#include <memory>
#include <string>

#include <API/Types/DataTypes.hpp>
#include <Operators/OperatorTypes.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/ptr_container/serialize_ptr_vector.hpp>

#include <boost/serialization/access.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

namespace NES {

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

class ExpressionStatment;
typedef std::shared_ptr<ExpressionStatment> ExpressionStatmentPtr;

enum class PredicateItemMutation {
    ATTRIBUTE,
    VALUE
};

class UserAPIExpression;
typedef std::shared_ptr<UserAPIExpression> UserAPIExpressionPtr;

class Predicate;
typedef std::shared_ptr<Predicate> PredicatePtr;

class Field;
typedef std::shared_ptr<Field> FieldPtr;

class UserAPIExpression {
  public:
    virtual ~UserAPIExpression(){};
    virtual const ExpressionStatmentPtr generateCode(GeneratedCodePtr& code) const = 0;
    virtual const std::string toString() const = 0;
    virtual UserAPIExpressionPtr copy() const = 0;
    virtual bool equals(const UserAPIExpression& rhs) const = 0;

  private:
    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
    }
};

class Predicate : public UserAPIExpression {
  public:
    Predicate(const BinaryOperatorType& op,
              const UserAPIExpressionPtr left,
              const UserAPIExpressionPtr right,
              const std::string& functionCallOverload,
              bool bracket = true);
    Predicate(const BinaryOperatorType& op,
              const UserAPIExpressionPtr left,
              const UserAPIExpressionPtr right,
              bool bracket = true);

    virtual const ExpressionStatmentPtr generateCode(GeneratedCodePtr& code) const override;
    virtual const std::string toString() const override;
    virtual UserAPIExpressionPtr copy() const override;
    bool equals(const UserAPIExpression& rhs) const override;
    BinaryOperatorType getOperatorType() const;
    const UserAPIExpressionPtr getLeft() const;
    const UserAPIExpressionPtr getRight() const;

  private:
    Predicate() = default;
    BinaryOperatorType op;
    UserAPIExpressionPtr left;
    UserAPIExpressionPtr right;
    bool bracket;
    std::string functionCallOverload;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(UserAPIExpression)
            & BOOST_SERIALIZATION_NVP(op)
            & BOOST_SERIALIZATION_NVP(left)
            & BOOST_SERIALIZATION_NVP(right)
            & BOOST_SERIALIZATION_NVP(bracket)
            & BOOST_SERIALIZATION_NVP(functionCallOverload);
    }
};

class PredicateItem : public UserAPIExpression {
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
    PredicateItem(const char* val);

    virtual const ExpressionStatmentPtr generateCode(GeneratedCodePtr& code) const override;
    virtual const std::string toString() const override;
    virtual UserAPIExpressionPtr copy() const override;

    bool equals(const UserAPIExpression& rhs) const override;

    bool isStringType() const;
    const DataTypePtr getDataTypePtr() const;
    AttributeFieldPtr getAttributeField() {
        return this->attribute;
    };
    const ValueTypePtr& getValue() const;

  private:
    PredicateItem() = default;
    PredicateItemMutation mutation;
    AttributeFieldPtr attribute = nullptr;
    ValueTypePtr value = nullptr;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(UserAPIExpression)
            & BOOST_SERIALIZATION_NVP(mutation)
            & BOOST_SERIALIZATION_NVP(attribute)
            & BOOST_SERIALIZATION_NVP(value);
    }
};

typedef std::shared_ptr<PredicateItem> PredicateItemPtr;

class Field : public PredicateItem {
  public:
    Field(AttributeFieldPtr name);

  private:
    std::string _name;
};

typedef std::shared_ptr<Field> FieldPtr;

const PredicatePtr createPredicate(const UserAPIExpression& expression);

Predicate operator==(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator!=(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator<(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator>(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator>=(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator<=(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator+(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator-(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator*(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator/(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator%(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator&&(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator||(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator&(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator|(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator^(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator<<(const UserAPIExpression& lhs, const UserAPIExpression& rhs);
Predicate operator>>(const UserAPIExpression& lhs, const UserAPIExpression& rhs);

Predicate operator==(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator!=(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator<(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator>(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator>=(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator<=(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator+(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator-(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator*(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator/(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator%(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator&&(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator||(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator&(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator|(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator^(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator<<(const PredicateItem& lhs, const UserAPIExpression& rhs);
Predicate operator>>(const PredicateItem& lhs, const UserAPIExpression& rhs);

Predicate operator==(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator!=(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator<(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator>(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator>=(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator<=(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator+(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator-(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator*(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator/(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator%(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator&&(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator||(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator&(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator|(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator^(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator<<(const UserAPIExpression& lhs, const PredicateItem& rhs);
Predicate operator>>(const UserAPIExpression& lhs, const PredicateItem& rhs);

Predicate operator==(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator!=(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator<(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator>(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator>=(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator<=(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator+(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator-(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator*(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator/(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator%(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator&&(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator||(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator&(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator|(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator^(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator<<(const PredicateItem& lhs, const PredicateItem& rhs);
Predicate operator>>(const PredicateItem& lhs, const PredicateItem& rhs);

}//end of namespace NES
#endif
