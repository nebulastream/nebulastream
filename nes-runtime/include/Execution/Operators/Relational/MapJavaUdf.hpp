/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDF_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <API/Schema.hpp>
#include <API/AttributeField.hpp>
#include <jni.h>

namespace NES::Runtime::Execution::Operators {

/**
* @brief Java udf map operator that evaluates a map expression on a input records.
* Map expressions read record fields, apply transformations, and can set/update fields.
*/
class MapJavaUdf : public ExecutableOperator {
  public:

  /**
   * Create java UDF function
   * @param className
   * @param byteCodeList
   * @param inputSchema
   * @param outputSchema
   * @param serializedInstance
   * @param methodName
   */
    MapJavaUdf(std::string className,
               std::unordered_map<std::string, std::vector<char>> byteCodeList,
               SchemaPtr inputSchema,
               SchemaPtr outputSchema,
               std::vector<char> serializedInstance,
               std::string methodName);

    /**
     * Init using jarPath currently only used for test purposes
     * @param jarPath
     */
    MapJavaUdf(std::string javaPath);

   void execute(ExecutionContext& ctx, Record& record) const override;

   /**
    * Return the java vm
    * @return JavaVM
    */
   JavaVM *getVM() {return jvm;};

   /**
    * Return the JNI environment
    * @return JNIEnv
    */
   JNIEnv *getEnvironment() {return env;};

   /**
    * Return the operators input schema
    * @return SchemaPtr
    */
   SchemaPtr getInputSchema(){ return inputSchema;};

   /**
    * Return the operators output schema
    * @return SchemaPtr
    */
   SchemaPtr getOutputSchema(){ return outputSchema;};

 private:
   std::string className;
   std::unordered_map<std::string, std::vector<char>> byteCodeList;
   SchemaPtr inputSchema;
   SchemaPtr outputSchema;
   std::vector<char> serializedInstance;
   std::string methodName;
   JavaVM* jvm;
   JNIEnv* env;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDF_HPP_
