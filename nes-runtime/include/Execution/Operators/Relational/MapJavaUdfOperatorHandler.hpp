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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDFOPERATORHANDLER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDFOPERATORHANDLER_HPP_

#ifdef ENABLE_JNI

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This handler stores states of a MapJavaUdf operator during its execution.
 */
class MapJavaUdfOperatorHandler : public OperatorHandler {
  public:
    /**
     * @brief This creates a MapJavaUdfOperatorHandler
     * @param className The java class name containing the java udf
     * @param methodName The java method name of the java udf
     * @param inputClassName The class name of the input type of the udf
     * @param outputClassName The class name of the output type of the udf
     * @param byteCodeList The byteCode containing serialized java objects to load into jvm
     * @param serializedInstance The serialized instance of the java java class
     * @param inputSchema The input schema of the records of the map function
     * @param outputSchema The output schema of the records of the map function
     * @param javaPath Optional: path to jar files to load classes from into JVM
     */
    explicit MapJavaUdfOperatorHandler(const std::string& className,
                                       const std::string& methodName,
                                       const std::string& inputClassName,
                                       const std::string& outputClassName,
                                       const std::unordered_map<std::string, std::vector<char>>& byteCodeList,
                                       const std::vector<char>& serializedInstance,
                                       SchemaPtr inputSchema,
                                       SchemaPtr outputSchema,
                                       const std::optional<std::string>& javaPath)
        : className(className), methodName(methodName), inputClassName(inputClassName), outputClassName(outputClassName),
          byteCodeList(byteCodeList), serializedInstance(serializedInstance), inputSchema(inputSchema),
          outputSchema(outputSchema), javaPath(javaPath) {}

    void start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) override {}
    void stop(QueryTerminationType, PipelineExecutionContextPtr) override {}

    /**
     * @brief This method returns the class name of the java udf
     * @return std::string class name
     */
    const std::string& getClassName() const { return className; }

    /**
     * @brief This method returns the method name of the java udf
     * @return std::string method name
     */
    const std::string& getMethodName() const { return methodName; }

    /**
     * @brief This method returns the class name of the input class name of the java udf
     * @return std::string input class name
     */
    const std::string& getInputClassName() const { return inputClassName; }

    /**
     * @brief This method returns the class name of the output class name of the java udf
     * @return std::string output class name
     */
    const std::string& getOutputClassName() const { return outputClassName; }

    /**
     * @brief This method returns the byte code list of the java udf
     * @return std::unordered_map<std::string, std::vector<char>> byte code list
     */
    const std::unordered_map<std::string, std::vector<char>>& getByteCodeList() const { return byteCodeList; }

    /**
     * @brief This method returns the serialized instance of the java udf
     * @return std::vector<char> serialized instance
     */
    const std::vector<char>& getSerializedInstance() const { return serializedInstance; }

    /**
     * @brief This method returns the input schema of the java udf
     * @return SchemaPtr input schema
     */
    const SchemaPtr& getInputSchema() const { return inputSchema; }

    /**
     * @brief This method returns the output schema of the java udf
     * @return SchemaPtr output schema
     */
    const SchemaPtr& getOutputSchema() const { return outputSchema; }

    /**
     * @brief This method returns the java path of the java udf jar
     * @return std::optional<std::string> java path
     */
    const std::optional<std::string>& getJavaPath() const { return javaPath; }

    /**
     * @brief This method returns the jni environment of the java udf
     * @return JNIEnv* java udf instance
     */
    JNIEnv* getEnvironment() const { return env; }

    /**
     * @brief This method sets the jni environment of the java udf
     * @param env jni environment
     */
    void setEnvironment(JNIEnv* env) { this->env = env; }

  private:
    const std::string className;
    const std::string methodName;
    const std::string inputClassName;
    const std::string outputClassName;
    const std::unordered_map<std::string, std::vector<char>> byteCodeList;
    const std::vector<char> serializedInstance;
    const SchemaPtr inputSchema;
    const SchemaPtr outputSchema;
    JNIEnv* env;
    const std::optional<std::string> javaPath;
};

}// namespace NES::Runtime::Execution::Operators
#endif//ENABLE_JNI
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDFOPERATORHANDLER_HPP_
