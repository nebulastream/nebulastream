#ifndef NES_INCLUDE_QUERYCOMPILER_RECORDHANDLER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_RECORDHANDLER_HPP_
#include <QueryCompiler/CCodeGenerator/CCodeGeneratorForwardRef.hpp>
#include <string>
#include <map>
namespace NES {

/**
 * @brief The record handler allows a generatable
 * operator to access and modify attributes of the current stream record.
 * Furthermore, it guarantees that modifications to record attributes are correctly tracked.
 */
class RecordHandler {
  public:

    static RecordHandlerPtr create();

    /**
     * @brief Registers a new attribute to the record handler.
     * @param name attribute name.
     * @param variable reference to the variable.
     */
    void registerAttribute(std::string name, ExpressionStatmentPtr variable);

    /**
     * @brief Checks a specific attribute was already registered.
     * @return name attribute name.
     */
    bool hasAttribute(std::string name);

    /**
     * @brief Returns the VariableReference to the particular attribute name.
     * @param name attribute name
     * @return VariableDeclarationPtr
     */
    ExpressionStatmentPtr getAttribute(std::string name);

  private:
    std::map<std::string, ExpressionStatmentPtr> variableMap;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_RECORDHANDLER_HPP_
