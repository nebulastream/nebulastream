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

#include "Util/JNI/JNI.hpp"
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <algorithm>
#include <jni.h>
#include <string>

namespace NES::Runtime::Execution::Operators {

JavaUDFOperatorHandler::JavaUDFOperatorHandler(const std::string& className,
                                               const std::string& methodName,
                                               const std::string& inputClassName,
                                               const std::string& outputClassName,
                                               const std::unordered_map<std::string, std::vector<char>>& byteCodeList,
                                               const std::vector<char>& serializedInstance,
                                               NES::SchemaPtr udfInputSchema,
                                               NES::SchemaPtr udfOutputSchema,
                                               const std::optional<std::string>& javaPath)
    : className(className), classJNIName(convertToJNIName(className)), methodName(methodName), inputClassName(inputClassName),
      inputClassJNIName(convertToJNIName(inputClassName)), outputClassName(outputClassName),
      outputClassJNIName(convertToJNIName(outputClassName)), byteCodeList(byteCodeList), serializedInstance(serializedInstance),
      udfInputSchema(std::move(udfInputSchema)), udfOutputSchema(std::move(udfOutputSchema)), javaPath(javaPath),
      udfMethodId(nullptr), udfInstance(nullptr) {
    auto& jvm = jni::JVM::get();
    if (!jvm.isInitialized()) {
        // jvm.addOption("-verbose:class");
        // jvm.addOption("-verbose:jni");
        if (javaPath.has_value()) {
            jvm.addClasspath(javaPath.value());
        }
        jvm.init();
    }
}

void JavaUDFOperatorHandler::setup() {
    auto env = jni::getEnv();
    // load bytecodes
    jni::loadClassesFromByteList(getByteCodeList());
    auto c1 = jni::findClass(getClassName());

    // Build function signature of map function
    std::string sig = "(L" + getInputClassName() + ";)L" + getOutputClassName() + ";";

    // Find udf function
    this->udfMethodId = env->GetMethodID(c1, getMethodName().c_str(), sig.c_str());
    jni::jniErrorCheck();

    jni::jobject instance;
    // The map udf class will be either loaded from a serialized instance or allocated using class information
    if (!getSerializedInstance().empty()) {
        // Load instance if defined
        instance = deserializeInstanceFromData(serializedInstance.data());
    } else {
        // Create instance object using class information
        jclass clazz = jni::findClass(getClassName());
        jni::jniErrorCheck();

        // Here we assume the default constructor is available
        auto constr = env->GetMethodID(clazz, "<init>", "()V");
        instance = env->NewObject(clazz, constr);
        jni::jniErrorCheck();
    }
    this->udfInstance = env->NewGlobalRef(instance);
}

const std::string JavaUDFOperatorHandler::convertToJNIName(const std::string& javaClassName) const {
    std::string copy = javaClassName;
    std::replace(copy.begin(), copy.end(), '.', '/');
    return copy;
}

const std::string& JavaUDFOperatorHandler::getClassName() const { return className; }

const std::string& JavaUDFOperatorHandler::getClassJNIName() const { return classJNIName; }

const std::string& JavaUDFOperatorHandler::getMethodName() const { return methodName; }
const std::string& JavaUDFOperatorHandler::getInputClassName() const { return inputClassName; }

const std::string& JavaUDFOperatorHandler::getInputClassJNIName() const { return inputClassJNIName; }

const std::string& JavaUDFOperatorHandler::getOutputClassName() const { return outputClassName; }

const std::string& JavaUDFOperatorHandler::getOutputClassJNIName() const { return outputClassJNIName; }

const std::unordered_map<std::string, std::vector<char>>& JavaUDFOperatorHandler::getByteCodeList() const { return byteCodeList; }

const std::vector<char>& JavaUDFOperatorHandler::getSerializedInstance() const { return serializedInstance; }

const SchemaPtr& JavaUDFOperatorHandler::getUdfInputSchema() const { return udfInputSchema; }

const SchemaPtr& JavaUDFOperatorHandler::getUdfOutputSchema() const { return udfOutputSchema; }

jmethodID JavaUDFOperatorHandler::getUDFMethodId() const { return udfMethodId; }

void JavaUDFOperatorHandler::start(NES::Runtime::Execution::PipelineExecutionContextPtr,
                                   NES::Runtime::StateManagerPtr,
                                   uint32_t) {}
void JavaUDFOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {}
JavaUDFOperatorHandler::~JavaUDFOperatorHandler() { jni::freeObject(udfInstance); }

}// namespace NES::Runtime::Execution::Operators
