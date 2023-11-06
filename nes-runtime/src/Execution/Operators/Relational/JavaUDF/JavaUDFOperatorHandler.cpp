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

#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <algorithm>
#include <jni.h>
#include <string>

namespace NES::Runtime::Execution::Operators {

JavaUDFOperatorHandler::JavaUDFOperatorHandler(const std::string& className,
                                               const std::string& methodName,
                                               const std::string& inputClassName,
                                               const std::string& outputClassName,
                                               const jni::JavaUDFByteCodeList& byteCodeList,
                                               const jni::JavaSerializedInstance& serializedInstance,
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
        jvm.addClasspath(JAVA_UDF_UTILS);
        if (javaPath.has_value()) {
            jvm.addClasspath(javaPath.value());
        }
        jvm.init();
    }
}

void JavaUDFOperatorHandler::setup() {
    auto env = jni::getEnv();
    setupClassLoader();
    injectClassesIntoClassLoader();

    // Find udf function
    std::string sig = "(L" + getInputClassJNIName() + ";)L" + getOutputClassJNIName() + ";";
    NES_DEBUG("Java UDF method signature: {}", sig);
    auto clazz = loadClass(getClassName());
    this->udfMethodId = jni::getMethod(clazz, getMethodName().c_str(), sig.c_str());

    jni::jobject instance;
    // The map udf class will be either loaded from a serialized instance or allocated using class information
    if (!getSerializedInstance().empty()) {
        // Load instance if defined
        instance = jni::deserializeInstance(serializedInstance);
    } else {
        // Create instance using default constructor
        auto constr = jni::getMethod(clazz, "<init>", "()V");
        instance = env->NewObject(clazz, constr);
        jni::jniErrorCheck();
    }
    this->udfInstance = env->NewGlobalRef(instance);
}

const std::string JavaUDFOperatorHandler::convertToJNIName(const std::string& javaClassName) {
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

const jni::JavaUDFByteCodeList& JavaUDFOperatorHandler::getByteCodeList() const { return byteCodeList; }

const jni::JavaSerializedInstance& JavaUDFOperatorHandler::getSerializedInstance() const { return serializedInstance; }

const SchemaPtr& JavaUDFOperatorHandler::getUdfInputSchema() const { return udfInputSchema; }

const SchemaPtr& JavaUDFOperatorHandler::getUdfOutputSchema() const { return udfOutputSchema; }

jmethodID JavaUDFOperatorHandler::getUDFMethodId() const { return udfMethodId; }

void JavaUDFOperatorHandler::start(NES::Runtime::Execution::PipelineExecutionContextPtr, uint32_t) {}
void JavaUDFOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {}
JavaUDFOperatorHandler::~JavaUDFOperatorHandler() {
    if (udfInstance) {
        jni::freeObject(udfInstance);
    }
    if (classLoader) {
        jni::freeObject(classLoader);
    }
}

jni::jclass JavaUDFOperatorHandler::loadClass(const std::string_view& className) const {
    if (!classLoader) {
        return jni::findClass(convertToJNIName(std::string(className)));
    }
    auto javaString = jni::createString(className);
    jni::jniErrorCheck();
    auto clazz = jni::getEnv()->CallObjectMethod(classLoader, loadClassMethod, javaString);
    jni::jniErrorCheck();
    freeObject(javaString);
    return static_cast<jclass>(clazz);
}

void JavaUDFOperatorHandler::setupClassLoader() {
    auto env = jni::getEnv();
    auto loaderClazz = jni::findClass("stream/nebula/UDFClassLoader");
    auto constructor = jni::getMethod(loaderClazz, "<init>", "()V");
    loadClassMethod = jni::getMethod(loaderClazz, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;");
    classLoader = env->NewObject(loaderClazz, constructor);
    jni::jniErrorCheck();
}

void JavaUDFOperatorHandler::injectClassesIntoClassLoader() const {
    auto env = jni::getEnv();
    for (auto& [className, byteCode] : byteCodeList) {
        jbyteArray jData = env->NewByteArray(byteCode.size());
        jni::jniErrorCheck();
        jbyte* jCode = env->GetByteArrayElements(jData, nullptr);
        jni::jniErrorCheck();
        std::memcpy(jCode, byteCode.data(), byteCode.size());// copy the byte array into the JVM byte array
        const auto jniName = convertToJNIName(className);
        NES_DEBUG("Injecting Java class into JVM: {}", className);
        env->DefineClass(jniName.c_str(), classLoader, jCode, (jint) byteCode.size());
        jni::jniErrorCheck();
        env->ReleaseByteArrayElements(jData, jCode, JNI_ABORT);
        jni::jniErrorCheck();
    }
}

}// namespace NES::Runtime::Execution::Operators
