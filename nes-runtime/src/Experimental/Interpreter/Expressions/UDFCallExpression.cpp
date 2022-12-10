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

#ifdef USE_JNI

#include "Nautilus/Interface/FunctionCall.hpp"
#include <Execution/Expressions/UDFCallExpression.hpp>
#include <jni.h>
namespace NES::Nautilus {

JavaVM* jvm;
JNIEnv* env;
jmethodID mid;
jclass cls2;
jobject object;

UDFCallExpression::UDFCallExpression(std::vector<ExpressionPtr> arguments,
                                     std::string javaClass,
                                     std::string javaFunction,
                                     std::string signature)
    : arguments(arguments) {
    if (jvm == nullptr) {

        // Pointer to native interface
        //================== prepare loading of Java VM ============================
        JavaVMInitArgs vm_args;                     // Initialization arguments
        JavaVMOption* options = new JavaVMOption[2];// JVM invocation options
        options[0].optionString = (char*) "-Djava.class.path=/launcher.jar";
        options[1].optionString = (char*) "-verbose:class";// where to find java .class
        vm_args.version = JNI_VERSION_1_2;                 // minimum Java version
        vm_args.nOptions = 2;                              // number of options
        vm_args.options = options;
        vm_args.ignoreUnrecognized = true;// invalid options make the JVM init fail
        //=============== load and initialize Java VM and JNI interface =============
        jint rc = JNI_CreateJavaVM(&jvm, (void**) &env, &vm_args);// YES !!
        //delete options;    // we then no longer need the initialisation options.
        if (rc != JNI_OK) {
            // TO DO: error processing...
            NES_DEBUG("rc != JNI_OK --> " << rc);
            exit(EXIT_FAILURE);
        }
    }
    std::cout << "JVM load succeeded: Version ";
    jint ver = env->GetVersion();
    std::cout << ((ver >> 16) & 0x0f) << "." << (ver & 0x0f) << std::endl;

    // TO DO: add the code that will use JVM <============  (see next steps)

    auto c1 = env->FindClass(javaClass.data());
    env->ExceptionDescribe();// try to find the class
    if (c1 == nullptr)
        std::cerr << "ERROR: class not found !" << std::endl;
    jmethodID constructor = env->GetMethodID(c1, "<init>", "()V");
    object = env->NewObject(c1, constructor);
    mid = env->GetMethodID(c1, javaFunction.c_str(), signature.c_str());
    if (mid == nullptr) {
        std::cerr << "ERROR: method " << javaFunction << "() not found !" << std::endl;
    } else {
    }
    jint param = 1;
    auto res = env->CallIntMethod(object, mid, param);
    std::cerr << "Result " << res << std::endl;
}

int64_t callJavaUDF_1(void* value1) {
    auto param = (jvalue*) value1;
    return env->CallLongMethod(object, mid, param[0]);
}

int64_t callJavaUDF_3(void* value1, void* value2, void* value3) {
    auto param1 = (jvalue*) value1;
    auto param2 = (jvalue*) value2;
    auto param3 = (jvalue*) value3;
    return env->CallLongMethod(object, mid, param1[0], param2[0], param3[0]);
}

int64_t callJavaUDF_4(void* value1, void* value2, void* value3, void* value4) {
    auto param1 = (jvalue*) value1;
    auto param2 = (jvalue*) value2;
    auto param3 = (jvalue*) value3;
    auto param4 = (jvalue*) value4;
    return env->CallLongMethod(object, mid, param1[0], param2[0], param3[0], param4[0]);
}

void* createJValue(int64_t value) {
    auto val = new jvalue;
    val->j = value;
    return val;
}

Value<> UDFCallExpression::execute(Record& record) {
    std::vector<Value<MemRef>> argumentValues;
    for (auto& arg : arguments) {
        auto value = arg->execute(record);
        auto jValue = FunctionCall<>("createJValue", createJValue, value.as<Int64>());
        argumentValues.emplace_back(jValue);
    }

    if (argumentValues.size() == 1) {
        return FunctionCall<>("callJavaUDF_1", callJavaUDF_1, argumentValues[0]);
    }
    if (argumentValues.size() == 3) {
        return FunctionCall<>("callJavaUDF_3", callJavaUDF_3, argumentValues[0], argumentValues[1], argumentValues[2]);
    }
    if (argumentValues.size() == 4) {
        return FunctionCall<>("callJavaUDF_4",
                              callJavaUDF_4,
                              argumentValues[0],
                              argumentValues[1],
                              argumentValues[2],
                              argumentValues[3]);
    }
    return FunctionCall<>("callJavaUDF_1", callJavaUDF_1, argumentValues[0]);
}

}// namespace NES::Nautilus

#endif

#ifdef USE_BABELFISH

#include <Execution/Expressions/UDFCallExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
namespace NES::Nautilus {

UDFCallExpression::UDFCallExpression(std::vector<ExpressionPtr> arguments, std::string, std::string, std::string)
    : arguments(arguments) {}

int64_t callJavaUDF_1(long) { NES_NOT_IMPLEMENTED(); }

int64_t callJavaUDF_3(long, long, long) { NES_NOT_IMPLEMENTED(); }

int64_t callJavaUDF_4(long, long, long, long) { NES_NOT_IMPLEMENTED(); }

Value<> UDFCallExpression::execute(Record& record) {
    std::vector<Value<>> argumentValues;
    for (auto& arg : arguments) {
        auto value = arg->execute(record);
        argumentValues.emplace_back(value);
    }

    if (argumentValues.size() == 1) {
        return FunctionCall<>("callJavaUDF_1", callJavaUDF_1, argumentValues[0].as<Int64>());
    }
    if (argumentValues.size() == 3) {
        return FunctionCall<>("callJavaUDF_3",
                              callJavaUDF_3,
                              argumentValues[0].as<Int64>(),
                              argumentValues[1].as<Int64>(),
                              argumentValues[2].as<Int64>());
    }
    if (argumentValues.size() == 4) {
        return FunctionCall<>("callJavaUDF_4",
                              callJavaUDF_4,
                              argumentValues[0].as<Int64>(),
                              argumentValues[1].as<Int64>(),
                              argumentValues[2].as<Int64>(),
                              argumentValues[3].as<Int64>());
    }
    return FunctionCall<>("callJavaUDF_1", callJavaUDF_1, argumentValues[0].as<Int64>());
}

}// namespace NES::Nautilus

#endif