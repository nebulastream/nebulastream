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

namespace NES::Runtime::Execution::Operators {

// Only used for test purposes
MapJavaUdf::MapJavaUdf(std::string javaPath) {
    // Start VM
    if (jvm == nullptr) {
        // init java vm arguments
        JavaVMInitArgs vm_args;                     // Initialization arguments
        JavaVMOption* options = new JavaVMOption[2];// JVM invocation options
        //options[0].optionString = (char*) ("-Djava.class.path=" + javaPath).c_str();
        options[0].optionString = (char*) "-Djava.class.path=/home/amichalke/nebulastream/udf";
        options[1].optionString = (char*) "-verbose:jni";
        options[2].optionString = (char*) "-verbose:class";// where to find java .class
        vm_args.nOptions = 3;                              // number of options
        vm_args.options = options;
        vm_args.version = JNI_VERSION_1_2; // minimum Java version
        vm_args.ignoreUnrecognized = true; // invalid options make the JVM init fail

        // create java VM anc check for errors
        jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);
        if (rc != JNI_OK) {
            // TODO: error processing
            std::cout << rc << std::endl;
            exit(EXIT_FAILURE);
        }
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();  // print the stack trace
            exit(1);
        }
    }
}

/**
 * Load classes without custom class loader
 * @param byteCodeList
 * @return
 */
std::vector<jclass> loadClassesFromByteList(void* operatorPtr, std::unordered_map<std::string, std::vector<char>> byteCodeList) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    std::vector<jclass> classes;
    // Load the classes from the byte list and the serialized instance
    for (auto entry : byteCodeList) {
        // TODO add the custom class loader
        auto bufLen = entry.second.size();
        const auto byteCode = reinterpret_cast<jbyte*>(&entry.second[0]);
        auto clazz = mapUDF->getEnvironment()->DefineClass(entry.first.data(), NULL, byteCode, (jsize) bufLen);
        classes.push_back(clazz);
    }
    return classes;
}

MapJavaUdf::MapJavaUdf(std::string className,
                       std::unordered_map<std::string, std::vector<char>> byteCodeList,
                       SchemaPtr inputSchema,
                       SchemaPtr outputSchema,
                       std::vector<char> serializedInstance,
                       std::string methodName)
    : className(className), byteCodeList(byteCodeList), inputSchema(inputSchema), outputSchema(outputSchema),
      serializedInstance(serializedInstance), methodName(methodName) {
    // Start VM
    if (jvm == nullptr) {
        // init java vm arguments
        JavaVMInitArgs vm_args;           // Initialization arguments
        vm_args.version = JNI_VERSION_1_2;// minimum Java version
        vm_args.ignoreUnrecognized = true;// invalid options make the JVM init fail

        // create java VM anc check for errors
        jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);
        if (rc != JNI_OK) {
            // TODO: error processing
            std::cout << rc << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    auto classes = loadClassesFromByteList(this, byteCodeList);
}

void* findClassProxyInput(void* operatorPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    jclass pojoClass = mapUDF->getEnvironment()->FindClass(mapUDF->getInputProxyName().c_str());
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "find class\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
    return pojoClass;
}

void* findClassProxyOutput(void* operatorPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    jclass pojoClass = mapUDF->getEnvironment()->FindClass(mapUDF->getOutputProxyName().c_str());
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "find class\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
    return pojoClass;
}

void* allocateObject(void* operatorPtr, void* pojoClassPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    jobject pojo = mapUDF->getEnvironment()->AllocObject(pojoClass);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "alloc obj\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
    return pojo;
}

void setIntField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, int value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "I");
    mapUDF->getEnvironment()->SetIntField(pojo, id, (jint) value);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "set int field\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
}

int getIntField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "I");
    int value = (int) mapUDF->getEnvironment()->GetIntField(pojo, id);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "get int field\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
    return value;
}

void setFloatField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, float value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "F");
    mapUDF->getEnvironment()->SetIntField(pojo, id, (jfloat) value);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "set float field\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
}

float getFloatField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "F");
    float value = (float) mapUDF->getEnvironment()->GetFloatField(pojo, id);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "get float field\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
    return value;
}

void setBooleanField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex, bool value) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "B");
    mapUDF->getEnvironment()->SetIntField(pojo, id, (jboolean) value);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "set bool field\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
}

float getBooleanField(void* operatorPtr, void* pojoClassPtr, void* pojoObjectPtr, int fieldIndex) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;
    auto pojoClass = (jclass) pojoClassPtr;
    auto pojo = (jobject) pojoObjectPtr;
    std::string fieldName = mapUDF->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = mapUDF->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), "B");
    bool value = (bool) mapUDF->getEnvironment()->GetBooleanField(pojo, id);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        std::cout << "get bool field\n";
        mapUDF->getEnvironment()->ExceptionDescribe();// print the stack trace
        exit(EXIT_FAILURE);
    }
    return value;
}

/**
 *
 * @param record
 * @return pointer to the output pojo containing the map result
 */
void* executeUdf(void* operatorPtr, void* pojoObjectPtr) {
    auto mapUDF = (MapJavaUdf*) operatorPtr;

    // find class implementing the map udf
    jclass c1 = mapUDF->getEnvironment()->FindClass(mapUDF->getClassName().c_str());
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        mapUDF->getEnvironment()->ExceptionDescribe();  // print the stack trace
        exit(EXIT_FAILURE);
    }

    // find the constructor method
    // We assume that the constructor
    jmethodID constructor = mapUDF->getEnvironment()->GetMethodID(c1, "<init>", "()V");
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        mapUDF->getEnvironment()->ExceptionDescribe();  // print the stack trace
        exit(EXIT_FAILURE);
    }
    std::cout << "found constructor\n";

    // Create obj by calling the constructor
    jobject object = mapUDF->getEnvironment()->NewObject(c1, constructor);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        mapUDF->getEnvironment()->ExceptionDescribe();  // print the stack trace
        exit(EXIT_FAILURE);
    }
    std::cout << "new object\n";

    // Build function signature of map function
    // Currently we assume that the pojos are *not* in java packages
    // if this changes it should be reflected in the signature here
    std::string sig = "(L" + mapUDF->getInputProxyName() + ";)L" + mapUDF->getOutputProxyName() + ";";

    // Find udf function
    jmethodID mid = mapUDF->getEnvironment()->GetMethodID(c1, mapUDF->getMethodName().c_str(), sig.c_str());
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        mapUDF->getEnvironment()->ExceptionDescribe();  // print the stack trace
        exit(EXIT_FAILURE);
    }
    std::cout << "found map\n";

    // Call udf function
    jobject udf_result = mapUDF->getEnvironment()->CallObjectMethod(object, mid, pojoObjectPtr);
    if (mapUDF->getEnvironment()->ExceptionOccurred()) {
        mapUDF->getEnvironment()->ExceptionDescribe();  // print the stack trace
        exit(EXIT_FAILURE);
    }
    return udf_result;
}

void MapJavaUdf::execute(ExecutionContext& ctx, Record& record) const {
    auto thisOperatorPtr = Value<MemRef>((std::int8_t *) this);

    // Create proxy input pojo and load record values
    auto inputClassPtr = FunctionCall<>("findClassProxyInput", findClassProxyInput, thisOperatorPtr);
    auto inputPojoPtr = FunctionCall<>("allocateObject", allocateObject, thisOperatorPtr, inputClassPtr);
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

    // Get proxy pojo and call Udf
    auto outputClassPtr = FunctionCall<>("findClassProxyOutput", findClassProxyOutput, thisOperatorPtr);
    auto outputPojoPtr =  FunctionCall<>("allocateObject", allocateObject, thisOperatorPtr, outputClassPtr);
    outputPojoPtr = FunctionCall<>("executeUdf", executeUdf, thisOperatorPtr, outputPojoPtr);

    // Write result into result record
    auto result = new Record();
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
            Value<> val =
                FunctionCall<>("getBooleanField", getBooleanField, thisOperatorPtr, outputClassPtr, outputPojoPtr, Value<Int32>(i));
            result->write(fieldName, val);
        }
    }

    // Trigger execution of next operator
    child->execute(ctx, (Record&) result);
}
}// namespace NES::Runtime::Execution::Operators