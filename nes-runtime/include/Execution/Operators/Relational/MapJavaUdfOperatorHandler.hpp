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

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <utility>

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
                                       const std::optional<std::string>& javaPath) : className(className),
                                                                      methodName(methodName),
                                                                      inputClassName(inputClassName),
                                                                      outputClassName(outputClassName),
                                                                      byteCodeList(byteCodeList),
                                                                      serializedInstance(serializedInstance),
                                                                      inputSchema(inputSchema),
                                                                      outputSchema(outputSchema),
                                                                      javaPath(javaPath) {}

    void start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) override {}
    void stop(QueryTerminationType, PipelineExecutionContextPtr) override {}

    const std::string& getClassName() const { return className; }

    const std::string& getMethodName() const { return methodName; }

    const std::string& getInputClassName() const { return inputClassName; }

    const std::string& getOutputClassName() const { return outputClassName; }

    const std::unordered_map<std::string, std::vector<char>>& getByteCodeList() const { return byteCodeList; }

    const std::vector<char>& getSerializedInstance() const { return serializedInstance; }

    const SchemaPtr& getInputSchema() const { return inputSchema; }

    const SchemaPtr& getOutputSchema() const { return outputSchema; }

    const std::optional<std::string>& getJavaPath() const { return javaPath; }

    JNIEnv* getEnvironment() const { return env; }

    void setEnvironment(JNIEnv* env) { MapJavaUdfOperatorHandler::env = env; }

  private:
    const std::string className;
    const std::string methodName;
    const std::string inputClassName;
    const std::string outputClassName;
    const std::unordered_map<std::string, std::vector<char>> byteCodeList;
    const std::vector<char> serializedInstance;
    const SchemaPtr inputSchema;
    const SchemaPtr outputSchema;
    JNIEnv *env;
    const std::optional<std::string> javaPath;
};

}// namespace NES::Runtime::Execution::Operators
#endif //NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_MAPJAVAUDFOPERATORHANDLER_HPP_
