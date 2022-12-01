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

#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <jni.h>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * Error checking JNI call. Call this function after each JNI call.
 * @param operatorPtr
 */
inline void jniErrorCheck(void* operatorPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        // print the stack trace
        mapUDF->getEnvironment()->ExceptionDescribe();
        NES_FATAL_ERROR("An error occured during a map java UDF execution");
        exit(EXIT_FAILURE);
    }
}

/**
 * Load classes from from the byteCodeList
 * @param operatorPtr
 * @param byteCodeList
 */
void loadClassesFromByteList(void* operatorPtr, const std::unordered_map<std::string, std::vector<char>>& byteCodeList) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    for (auto entry : byteCodeList) {
        auto bufLen = entry.second.size();
        const auto byteCode = reinterpret_cast<jbyte*>(&entry.second[0]);
        mapUDF->getEnvironment()->DefineClass(entry.first.data(), nullptr, byteCode, (jsize) bufLen);
    }
}

/**
 * Deserialize the instance using the helper deserialize java function in MapJavaUdfUtils
 * @param operatorPtr
 * @return jobject
 */
// https://docs.oracle.com/javase/8/docs/platform/serialization/spec/input.html
jobject loadSerializedInstance(void* operatorPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto clazz = mapUDF->getEnvironment()->FindClass("MapJavaUdfUtils");
    jniErrorCheck(mapUDF);
    auto mid = mapUDF->getEnvironment()->GetMethodID(clazz, "deserialize", "(Ljava/nio/ByteBuffer)Ljava/lang/Object");
    jniErrorCheck(mapUDF);
    auto obj = mapUDF->getEnvironment()->CallStaticObjectMethod(clazz, mid);
    jniErrorCheck(mapUDF);
    return obj;

    //auto mapUDF = (MapJavaUdf*) operatorPtr;
    // create NewDirectByteBuffer in JNI which takes the byte buffer
    //jobject buffer = mapUDF->getEnvironment()->NewDirectByteBuffer(operatorPtr->, jlong capacity);
    // Call bytearrayinputstream(byte[])
    // Call ObjectInputStream(InputStream in)
    // Call ObjectInputStream.readObject()
    // load with class path?
}

MapJavaUdf::MapJavaUdf(const std::string& className,
                       const std::string& methodName,
                       const std::string& inputProxyName,
                       const std::string& outputProxyName,
                       const std::unordered_map<std::string, std::vector<char>>& byteCodeList,
                       const std::vector<char>& serializedInstance,
                       SchemaPtr inputSchema,
                       SchemaPtr outputSchema,
                       const std::string& javaPath)
    : className(className), methodName(methodName), inputProxyName(inputProxyName), outputProxyName(outputProxyName),
      byteCodeList(byteCodeList), serializedInstance(serializedInstance), inputSchema(inputSchema), outputSchema(outputSchema) {
    // Start VM
    if (jvm == nullptr) {
        // init java vm arguments
        JavaVMInitArgs vm_args;             // Initialization arguments
        auto* options = new JavaVMOption[2];// JVM invocation options
        options[0].optionString = (char*) ("-Djava.class.path=" + javaPath).c_str();
        options[1].optionString = (char*) "-verbose:jni";
        options[2].optionString = (char*) "-verbose:class";// where to find java .class
        vm_args.nOptions = 3;                              // number of options
        vm_args.options = options;
        vm_args.version = JNI_VERSION_1_2;// minimum Java version
        vm_args.ignoreUnrecognized = true;// invalid options make the JVM init fail

        jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);
        if (rc == JNI_OK) {
            NES_TRACE("Java VM startup was successful");
        } else if (rc == JNI_ERR) {
            NES_FATAL_ERROR("An unknown error occured during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EDETACHED) {
            NES_FATAL_ERROR("Thread detached from the VM during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EVERSION) {
            NES_FATAL_ERROR("A JNI version error occured during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_ENOMEM) {
            NES_FATAL_ERROR("Not enough memory during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EEXIST) {
            NES_FATAL_ERROR("Java VM already exists!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EINVAL) {
            NES_FATAL_ERROR("Invalid arguments during Java VM startup!");
            exit(EXIT_FAILURE);
        }
    }
}

MapJavaUdf::MapJavaUdf(const std::string& className,
                       const std::string& methodName,
                       const std::string& inputProxyName,
                       const std::string& outputProxyName,
                       const std::unordered_map<std::string, std::vector<char>>& byteCodeList,
                       const std::vector<char>& serializedInstance,
                       SchemaPtr inputSchema,
                       SchemaPtr outputSchema)
    : className(className), methodName(methodName), inputProxyName(inputProxyName), outputProxyName(outputProxyName),
      byteCodeList(byteCodeList), serializedInstance(serializedInstance), inputSchema(inputSchema), outputSchema(outputSchema) {
    // Start VM
    if (jvm == nullptr) {
        // init java vm arguments
        JavaVMInitArgs vm_args;
        vm_args.version = JNI_VERSION_1_2;// minimum Java version
        vm_args.ignoreUnrecognized = true;// invalid options make the JVM init fail

        jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);
        if (rc == JNI_OK) {
            NES_TRACE("Java VM startup was successful");
        } else if (rc == JNI_ERR) {
            NES_FATAL_ERROR("An unknown error occured during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EDETACHED) {
            NES_FATAL_ERROR("Thread detached from the VM during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EVERSION) {
            NES_FATAL_ERROR("A JNI version error occured during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_ENOMEM) {
            NES_FATAL_ERROR("Not enough memory during Java VM startup!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EEXIST) {
            NES_FATAL_ERROR("Java VM already exists!");
            exit(EXIT_FAILURE);
        } else if (rc == JNI_EINVAL) {
            NES_FATAL_ERROR("Invalid arguments during Java VM startup!");
            exit(EXIT_FAILURE);
        }
    }
    loadClassesFromByteList(this, byteCodeList);
}

MapJavaUdf::~MapJavaUdf() { jvm->DestroyJavaVM(); }

void* findClassProxyInput(void* operatorPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    jclass pojoClass = mapUDF->getEnvironment()->FindClass(mapUDF->getInputProxyName().c_str());
    jniErrorCheck(mapUDF);
    return pojoClass;
}

void* findClassProxyOutput(void* operatorPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    jclass pojoClass = mapUDF->getEnvironment()->FindClass(mapUDF->getOutputProxyName().c_str());
    jniErrorCheck(mapUDF);
    return pojoClass;
}

void* allocateObject(void* operatorPtr, void* pojoClassPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    jobject pojo = mapUDF->getEnvironment()->AllocObject(pojoClass);
    jniErrorCheck(mapUDF);
    return pojo;
}

void setIntField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, int value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "I");
    jniErrorCheck(mapUDF);
    mapUDF->getEnvironment()->SetIntField(pojo, id, (jint) value);
    jniErrorCheck(mapUDF);
}

int getIntField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "I");
    jniErrorCheck(mapUDF);
    int value = (int) mapUDF->getEnvironment()->GetIntField(pojo, id);
    jniErrorCheck(mapUDF);
    return value;
}

void setFloatField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, float value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "F");
    jniErrorCheck(mapUDF);
    mapUDF->getEnvironment()->SetFloatField(pojo, id, (jfloat) value);
    jniErrorCheck(mapUDF);
}

float getFloatField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "F");
    jniErrorCheck(mapUDF);
    float value = (float) mapUDF->getEnvironment()->GetFloatField(pojo, id);
    jniErrorCheck(mapUDF);
    return value;
}

void setBooleanField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, bool value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "B");
    jniErrorCheck(mapUDF);
    mapUDF->getEnvironment()->SetIntField(pojo, id, (jboolean) value);
    jniErrorCheck(mapUDF);
}

float getBooleanField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "B");
    jniErrorCheck(mapUDF);
    bool value = (bool) mapUDF->getEnvironment()->GetBooleanField(pojo, id);
    jniErrorCheck(mapUDF);
    return value;
}

void setStringField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, void *value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    jstring string = mapUDF->getEnvironment()->NewStringUTF((char *)value);
    jniErrorCheck(mapUDF);
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jniErrorCheck(mapUDF);
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "Ljava.lang.String");
    jniErrorCheck(mapUDF);
    mapUDF->getEnvironment()->SetObjectField(pojo, id, (jobject) string);
    jniErrorCheck(mapUDF);
}

std::string getStringField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "Ljava.lang.String");
    jstring value = (jstring) mapUDF->getEnvironment()->GetObjectField(pojo, id);
    jniErrorCheck(mapUDF);
    int lenght = 1; // TODO derrive lenght
    jboolean isCopy;
    const char *convertedValue = mapUDF->getEnvironment()->GetStringUTFChars(value, &isCopy);
    jniErrorCheck(mapUDF);
    return std::string(convertedValue, lenght);
}

// TODO: Build a wrapper around field access
void setField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, void* value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto field = mapUDF->getInputSchema()->fields[fieldIndex];

    if (field->getDataType()->isInteger()) {
        int *val = (int*)(value);
        setIntField(operatorPtr, pojoClassPtr, pojoObjectPtr, Value<Int32>(0), (int)(*val));
    } else if (field->getDataType()->isFloat()) {
        //...
    } else if (field->getDataType()->isBoolean()) {

    } else if (field->getDataType()->isText()) {

    }
}

/**
 *
 * @param operatorPtr
 * @param pojoObjectPtr
 * @return
 */
void* executeUdf(void* operatorPtr, void* pojoObjectPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;

    // load instance
    jobject instance = loadSerializedInstance(operatorPtr);

    // find class implementing the map udf
    jclass c1 = mapUDF->getEnvironment()->FindClass(mapUDF->getClassName().c_str());
    jniErrorCheck(mapUDF);

    // Build function signature of map function
    std::string sig = "(L" + mapUDF->getInputProxyName() + ";)L" + mapUDF->getOutputProxyName() + ";";

    // Find udf function
    jmethodID mid = mapUDF->getEnvironment()->GetMethodID(c1, mapUDF->getMethodName().c_str(), sig.c_str());
    jniErrorCheck(mapUDF);
    std::cout << "found map\n";

    // Call udf function
    jobject udf_result = mapUDF->getEnvironment()->CallObjectMethod(instance, mid, pojoObjectPtr);
    jniErrorCheck(mapUDF);
    return udf_result;
}

void MapJavaUdf::execute(ExecutionContext& ctx, Record& record) const {
    auto thisOperatorPtr = Value<MemRef>((std::int8_t*) this);

    // Create proxy input pojo and load record values
    auto inputClassPtr = FunctionCall<>("findClassProxyInput", findClassProxyInput, thisOperatorPtr);
    auto inputPojoPtr = FunctionCall<>("allocateObject", allocateObject, thisOperatorPtr, inputClassPtr);

    // Loading record values into java objects
    // 1. If the input schema contains only one field we have a primitive type (String, Integer, ..)
    // 2. Otherwise we have a plain old java object containing the multiple primitive types
    if (inputSchema->fields.size() == 1) {
        // 1. Primitive type as map input
        auto field = inputSchema->fields[0];
        auto fieldName = field->getName();

        if (field->getDataType()->isInteger()) {
            FunctionCall<>("setIntField",
                           setIntField,
                           thisOperatorPtr,
                           inputClassPtr,
                           inputPojoPtr,
                           Value<Int32>(0),
                           record.read(fieldName).as<Int32>());
        } else if (field->getDataType()->isFloat()) {
            FunctionCall<>("setFloatField",
                           setFloatField,
                           thisOperatorPtr,
                           inputClassPtr,
                           inputPojoPtr,
                           Value<Int32>(0),
                           record.read(fieldName).as<Float>());
        } else if (field->getDataType()->isBoolean()) {
            FunctionCall<>("setBooleanField",
                           setBooleanField,
                           thisOperatorPtr,
                           inputClassPtr,
                           inputPojoPtr,
                           Value<Int32>(0),
                           record.read(fieldName).as<Boolean>());
        } else if (field->getDataType()->isText()) {
            /*FunctionCall<>("setStringField",
                           setStringField,
                           thisOperatorPtr,
                           inputClassPtr,
                           inputPojoPtr,
                           Value<Int32>(0),
                           record.read(fieldName).as<TextValue>());*/
        }
    } else {
        // 2. Plain old java object as map input
        for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
            auto field = inputSchema->fields[i];
            auto fieldName = field->getName();

            if (field->getDataType()->isInteger()) {
                FunctionCall<>("setIntField",
                               setIntField,
                               thisOperatorPtr,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int32>());
            } else if (field->getDataType()->isFloat()) {
                FunctionCall<>("setFloatField",
                               setFloatField,
                               thisOperatorPtr,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Float>());
            } else if (field->getDataType()->isBoolean()) {
                FunctionCall<>("setBooleanField",
                               setBooleanField,
                               thisOperatorPtr,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Boolean>());
            }
        }
    }

    // Get proxy pojo and call Udf
    auto outputClassPtr = FunctionCall<>("findClassProxyOutput", findClassProxyOutput, thisOperatorPtr);
    auto outputPojoPtr = FunctionCall<>("executeUdf", executeUdf, thisOperatorPtr, inputPojoPtr);

    // Write result into result record
    auto result = new Record();
    //if not primitive typexecute {
    for (int i = 0; i < (int) outputSchema->fields.size(); i++) {
        auto field = outputSchema->fields[i];
        auto fieldName = field->getName();

        if (field->getDataType()->isInteger()) {
            Value<> val =
                FunctionCall<>("getIntField", getIntField, thisOperatorPtr, outputClassPtr, outputPojoPtr, Value<Int32>(i));
            result->write(fieldName, val);
        } else if (field->getDataType()->isFloat()) {
            Value<> val =
                FunctionCall<>("getFloatField", getFloatField, thisOperatorPtr, outputClassPtr, outputPojoPtr, Value<Int32>(i));
            result->write(fieldName, val);
        } else if (field->getDataType()->isBoolean()) {
            Value<> val = FunctionCall<>("getBooleanField",
                                         getBooleanField,
                                         thisOperatorPtr,
                                         outputClassPtr,
                                         outputPojoPtr,
                                         Value<Int32>(i));
            result->write(fieldName, val);
        }
    }
    // else

    // relase objects auch strings in pojo

    // Trigger execution of next operator
    child->execute(ctx, (Record&) result);
}
}// namespace NES::Runtime::Execution::Operators