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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <jni.h>

namespace NES::Runtime::Execution::Operators {

// Because one jvm corresponds to one thread we need to have jvm as a global variable
// https://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/invocation.html
JavaVM* jvm = nullptr;

/**
* @brief Java udf map operator that evaluates a map expression on a input records.
* Map expressions read record fields, apply transformations, and can set/update fields.
*/
class MapJavaUdf : public ExecutableOperator {
  public:
    /**
     *
     * @param className
     * @param methodName
     * @param inputProxyName
     * @param outputProxyName
     * @param byteCodeList
     * @param serializedInstance
     * @param inputSchema
     * @param outputSchema
     * @param javaPath
     */
    MapJavaUdf(const std::string& className,
               const std::string& methodName,
               const std::string& inputProxyName,
               const std::string& outputProxyName,
               const std::unordered_map<std::string, std::vector<char>>& byteCodeList,
               const std::vector<char>& serializedInstance,
               SchemaPtr inputSchema,
               SchemaPtr outputSchema,
               const std::string& javaPath);

    /**
     *
     * @param className
     * @param methodName
     * @param inputProxyName
     * @param outputProxyName
     * @param byteCodeList
     * @param serializedInstance
     * @param inputSchema
     * @param outputSchema
     */
    MapJavaUdf(const std::string& className,
               const std::string& methodName,
               const std::string& inputProxyName,
               const std::string& outputProxyName,
               const std::unordered_map<std::string, std::vector<char>>& byteCodeList,
               const std::vector<char>& serializedInstance,
               SchemaPtr inputSchema,
               SchemaPtr outputSchema);

    /**
     * Deconstructor of MapJavaUdf
     */
    ~MapJavaUdf() override;

    /**
     *
     * @param ctx
     * @param record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

    /**
    * Return the JNI environment
    * @return JNIEnv
    */
    JNIEnv* getEnvironment() { return env; };

    /**
    * Return the operators input schema
    * @return SchemaPtr
    */
    SchemaPtr getInputSchema() { return inputSchema; };

    /**
    * Return the operators output schema
    * @return SchemaPtr
    */
    SchemaPtr getOutputSchema() { return outputSchema; };

    /**
    * Return the name with pkg prefix of the class containing the udf
    * @return std::string
    */
    std::string getClassName() { return className; };

    /**
    * Return the name of the method implementing the udf
    * @return std::string
    */
    std::string getMethodName() { return methodName; };

    /**
    * Return the name with pkg prefix of the object used as udf input value
    * @return std::string
    */
    std::string getInputProxyName() { return inputProxyName; };

    /**
     * Return the name with pkg prefix of the object used as udf output value
     * @return std::string
     */
    std::string getOutputProxyName() { return outputProxyName; };

    /**
     * Return the serialized instance of the class implementing the UDF
     * @return
     */
    std::vector<char> getSerializedInstance() { return serializedInstance; };

    /**
     * For test purposes
     * @return
     */
    jobject deserializeInstance(jobject object);

    /**
     * For test purposes
     * @return
     */
    jobject serializeInstance(jobject object);

  private:
    std::string className;
    std::string methodName;

    std::string inputProxyName;
    std::string outputProxyName;

    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

    SchemaPtr inputSchema;
    SchemaPtr outputSchema;

    JNIEnv* env;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDF_HPP_
