// LogicalMeosOperator.hpp

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALmeosOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALmeosOPERATOR_HPP_

#include <Operators/AbstractOperators/Arity/QuaternaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalQuaternaryOperator.hpp>
#include <string>

namespace NES {
namespace MeosOperator {

/**
         * @brief Logical Meos operator, defines three output schemas
         */
class LogicalMeosOperator : public LogicalOperator, public LogicalQuaternaryOperator {
  public:
    // Constructor
    LogicalMeosOperator(const ExpressionNodePtr left,
                        const ExpressionNodePtr middle,
                        const ExpressionNodePtr right,
                        const std::string function,
                        OperatorId id);

    // Destructor
    ~LogicalMeosOperator() override;

    // Override pure virtual functions from LogicalOperator
    OperatorPtr copy() override;
    bool inferSchema() override;
    void inferInputOrigins() override;
    std::vector<OperatorPtr> getLeftOperators() const override;
    std::vector<OperatorPtr> getRightOperators() const override;
    ExpressionNodePtr getLeft() const;
    ExpressionNodePtr getMiddle() const;
    ExpressionNodePtr getRight() const;
    std::string getFunction() const;

    void inferStringSignature() override;

  private:
    std::vector<OperatorPtr> getOperatorsBySchema(const SchemaPtr& schema) const;

    // Member variables
    ExpressionNodePtr left;
    ExpressionNodePtr middle;
    ExpressionNodePtr right;
    std::string function;

    std::vector<OriginId> leftInputOriginIds;
    std::vector<OriginId> rightInputOriginIds;
    std::string stringSignature;
};

}// namespace MeosOperator
}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALmeosOPERATOR_HPP_
