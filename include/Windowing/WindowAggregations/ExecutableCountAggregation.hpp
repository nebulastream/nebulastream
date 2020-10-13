#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLECOUNTAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLECOUNTAGGREGATION_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <utility>
#include <Windowing/WindowAggregations/ExecutableWindowAggregation.hpp>
#include <memory>
namespace NES{

typedef uint64_t CountType;


class ExecutableCountAggregation : public  ExecutableWindowAggregation<CountType, CountType, CountType>{
  public:
    ExecutableCountAggregation(AttributeFieldPtr onField, AttributeFieldPtr asField): ExecutableWindowAggregation<CountType, CountType, CountType>(onField, asField){
    };

    static std::shared_ptr<ExecutableWindowAggregation<CountType, CountType, CountType>> create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField){
        return std::make_shared<ExecutableCountAggregation>(std::move(onField), std::move(asField));
    };

    /*
    * @brief generate the code for lift and combine of the MaxAggregationDescriptor aggregate
    * @param currentCode
    * @param expressionStatement
    * @param inputStruct
    * @param inputRef
    */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement partialRef, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override{
            auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
            auto ifStatement = IF(
                partialRef < inputRef.accessRef(VarRefStatement(varDeclInput)),
                assign(partialRef, inputRef.accessRef(VarRefStatement(varDeclInput))));
            currentCode->addStatement(ifStatement.createCopy());
    };

    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    CountType lift(CountType) override {
        return 1;
    }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    CountType combine(CountType partialValue, CountType inputValue) override {
       return partialValue + inputValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    CountType lower(CountType partialAggregateValue) override {
        return partialAggregateValue;
    }

  protected:

};

}

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLECOUNTAGGREGATION_HPP_
