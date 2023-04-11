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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_FlatMapJavaUdfOperatorHandler_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_FlatMapJavaUdfOperatorHandler_HPP_

#ifdef ENABLE_JNI

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
//#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This handler stores states of a FlatMapJavaUdf operator during its execution.
 */
class FlatMapJavaUdfOperatorHandler /*: public JavaUDFOperatorHandler*/ {
  public:
/**
     * @brief This creates a FlatMapJavaUdfOperatorHandler
     * @param className The java class name containing the java udf
     * @param methodName The java method name of the java udf
     * @param inputClassName The class name of the input type of the udf
     * @param outputClassName The class name of the output type of the udf
     * @param byteCodeList The byteCode containing serialized java objects to load into jvm
     * @param serializedInstance The serialized instance of the java java class
     * @param inputSchema The input schema of the records of the flat map function
     * @param outputSchema The output schema of the records of the flat map function
     * @param javaPath Optional: path to jar files to load classes from into JVM
     *//*

    explicit FlatMapJavaUdfOperatorHandler(const std::string& className,
                                       const std::string& methodName,
                                       const std::string& inputClassName,
                                       const std::string& outputClassName,
                                       const std::unordered_map<std::string, std::vector<char>>& byteCodeList,
                                       const std::vector<char>& serializedInstance,
                                       SchemaPtr inputSchema,
                                       SchemaPtr outputSchema,
                                       const std::optional<std::string>& javaPath)
        : JavaUDFOperatorHandler(className, methodName, inputClassName, outputClassName, byteCodeList, serializedInstance, inputSchema, outputSchema, javaPath) {}

    */
/**
     * @brief This method returns the java udf object state
     * @return jobject java udf object
     *//*

    jobject getFlatMapObject() const { return flatMapObject; }

    */
/**
     * @brief This method sets the java udf object state
     * @param flatMapObject java udf object
     *//*

    void setFlatMapObject(jobject flatMapObject) { this->flatMapObject = flatMapObject; }

    */
/**
     * @brief This method returns the java udf method id
     * @return jmethodID java udf method id
     *//*

    jmethodID getFlatMapMethodId() const { return flatMapMethodId; }

    */
/**
     * @brief This method sets the java udf method id
     * @param flatMapMethodId java udf method id
     *//*

    void setFlatMapMethodId(const jmethodID flatMapMethodId) { this->flatMapMethodId = flatMapMethodId; }

  private:
    jmethodID flatMapMethodId;
    jobject flatMapObject;*/
};

}// namespace NES::Runtime::Execution::Operators
#endif//ENABLE_JNI
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_FlatMapJavaUdfOperatorHandler_HPP_
