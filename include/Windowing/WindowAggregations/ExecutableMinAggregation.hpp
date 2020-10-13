#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEMINAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEMINAGGREGATION_HPP_
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

template<typename InputType>
class ExecutableMinAggregation : public  ExecutableWindowAggregation<InputType, InputType, InputType>{
  public:
    ExecutableMinAggregation(AttributeFieldPtr onField, AttributeFieldPtr asField): ExecutableWindowAggregation<InputType, InputType, InputType>(onField, asField){
    };

    static std::shared_ptr<ExecutableWindowAggregation<InputType, InputType, InputType>> create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField){
        return std::make_shared<ExecutableMinAggregation<InputType>>(std::move(onField), std::move(asField));
    };

    /*
    * @brief generate the code for lift and combine of the MaxAggregationDescriptor aggregate
    * @param currentCode
    * @param expressionStatement
    * @param inputStruct
    * @param inputRef
    */
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement expressionStatement, StructDeclaration inputStruct, BinaryOperatorStatement inputRef) override{
        auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
        auto ifStatement = IF(
            expressionStatement > inputRef.accessRef(VarRefStatement(varDeclInput)),
            assign(expressionStatement, inputRef.accessRef(VarRefStatement(varDeclInput))));
        currentCode->addStatement(ifStatement.createCopy());
    };

    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    InputType lift(InputType inputValue) override {
        return inputValue;
    }

    /*
     * @brief combines two partial aggregates to a new partial aggregate
     * @param current partial value
     * @param the new input element
     * @return new partial aggregate as combination of partialValue and inputValue
     */
    InputType combine(InputType partialValue, InputType inputValue) override {
        if (inputValue < partialValue) {
            partialValue = inputValue;
        }
        return partialValue;
    }

    /*
     * @brief maps partial aggregates to an element of FinalAggregationType
     * @param partial aggregate element
     * @return element mapped to FinalAggregationType
     */
    InputType lower(InputType partialAggregateValue) override {
        return partialAggregateValue;
    }

  protected:

};

}

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEMINAGGREGATION_HPP_
