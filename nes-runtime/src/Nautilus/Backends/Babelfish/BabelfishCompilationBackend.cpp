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

#include "Nautilus/Backends/Babelfish/BabelfishCode.hpp"
#include <Execution/Operators/Relational/JavaUDF/JVMContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishCompilationBackend.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishExecutable.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishLoweringProvider.hpp>
#include <Nautilus/Util/Dyncall.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <cstdint>
#include <jni.h>
#include <string>
#include <type_traits>

namespace NES::Nautilus::Backends::Babelfish {

[[maybe_unused]] static CompilationBackendRegistry::Add<BabelfishCompilationBackend> babelfishBackend("BabelfishCompiler");
#pragma GCC diagnostic ignored "-Wunused-parameter"
JNIEXPORT void JNICALL DyncallReset(JNIEnv* env, jclass) {
    BC::Dyncall::getVM().reset();
    NES_DEBUG("Call DyncallReset");
}
JNIEXPORT void JNICALL DyncallSetArgB(JNIEnv* env, jclass, jboolean value) {
    NES_DEBUG("Call DyncallSetArgB");
    BC::Dyncall::getVM().addArgB(value);
}
JNIEXPORT void JNICALL DyncallSetArgI8(JNIEnv* env, jclass, jbyte value) {
    NES_DEBUG("Call DyncallSetArgI8");
    BC::Dyncall::getVM().addArgI8(value);
}
JNIEXPORT void JNICALL DyncallSetArgI16(JNIEnv* env, jclass, jshort value) {
    NES_DEBUG("Call DyncallSetArgI16");
    BC::Dyncall::getVM().addArgI16(value);
}
JNIEXPORT void JNICALL DyncallSetArgI32(JNIEnv* env, jclass, jint value) {
    NES_DEBUG("Call DyncallSetArgI32");
    BC::Dyncall::getVM().addArgI32(value);
}
JNIEXPORT void JNICALL DyncallSetArgI64(JNIEnv* env, jclass, jlong value) {
    NES_DEBUG("Call DyncallSetArgI64");
    BC::Dyncall::getVM().addArgI64(value);
}
JNIEXPORT void JNICALL DyncallSetArgF(JNIEnv* env, jclass, jfloat value) {
    NES_DEBUG("Call DyncallSetArgF");
    BC::Dyncall::getVM().addArgF(value);
}
JNIEXPORT void JNICALL DyncallSetArgD(JNIEnv* env, jclass, jdouble value) {
    NES_DEBUG("Call DyncallSetArgD");
    BC::Dyncall::getVM().addArgD(value);
}

JNIEXPORT void JNICALL DyncallSetArgPtr(JNIEnv* env, jclass, jlong value) {
    NES_DEBUG("Call DyncallSetPtr");
    auto ptr = (void*) value;
    BC::Dyncall::getVM().addArgPtr(ptr);
}

JNIEXPORT void JNICALL CallVoid(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallVoid");
    auto address = (void*) fptr;
    BC::Dyncall::getVM().callVoid(address);
}
JNIEXPORT jbyte JNICALL CallI8(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallI8");
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI8(address);
}
JNIEXPORT jshort JNICALL CallI16(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallI16");
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI16(address);
}
JNIEXPORT jint JNICALL CallI32(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallI32");
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI32(address);
}
JNIEXPORT jlong JNICALL CallI64(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallI64");
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI64(address);
}
JNIEXPORT jfloat JNICALL CallFloat(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallFloat");
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callF(address);
}
JNIEXPORT jdouble JNICALL CallDouble(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallDouble");
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callD(address);
}

JNIEXPORT jlong JNICALL CallPtr(JNIEnv* env, jclass, jlong fptr) {
    NES_DEBUG("Call CallPtr");
    auto address = (void*) fptr;
    auto resultPtr = BC::Dyncall::getVM().callPtr(address);
    return (int64_t) resultPtr;
}

void checkAndThrowException(JNIEnv* env) {
    if (env->ExceptionCheck()) {
        jthrowable exception = env->ExceptionOccurred();
        env->ExceptionClear();

        jclass exceptionClass = env->GetObjectClass(exception);
        jmethodID toStringMethod = env->GetMethodID(exceptionClass, "toString", "()Ljava/lang/String;");
        jobject description = env->CallObjectMethod(exception, toStringMethod);

        const char* utfDescription = env->GetStringUTFChars((jstring) description, nullptr);
        std::string errorMessage = utfDescription;
        env->ReleaseStringUTFChars((jstring) description, utfDescription);

        env->DeleteLocalRef(description);
        env->DeleteLocalRef(exceptionClass);

        throw std::runtime_error(errorMessage);
    }
}

BabelfishCompilationBackend::VM::VM() : env() {

    // /home/pgrulich/projects/graal/vm/latest_graalvm/graalvm-ce-java17-22.3.1/bin/java
    // --add-exports=org.graalvm.sdk/org.graalvm.polyglot=ALL-UNNAMED
    // -Dtruffle.class.path.append=/home/pgrulich/projects/nes/nautilusbf/language/target/classes:/home/pgrulich/.m2/repository/com/googlecode/json-simple/json-simple/1.1/json-simple-1.1.jar
    // -Dgraal.Dump=Truffle:1 -Dgraal.PrintGraph=Network
    // -Dgraal.GenLoopSafepoints=false
    // -Didea.test.cyclic.buffer.size=1048576

    std::string classpath = "/home/pgrulich/projects/nes/nautilusbf/language/target/language.jar:/home/pgrulich/projects/nes/"
                            "nautilusbf/launcher/target/launcher.jar";
    JavaVMInitArgs args{};
    std::vector<std::string> opt{
        //"-verbose:jni", "-verbose:class",
            "-Dgraal.Dump=Truffle:1",
            "-Dgraal.PrintGraph=Network",
        "-Dgraal.GenLoopSafepoints=false",
        "-Dgraalvm.locatorDisabled=true",
        //    "-Dtruffle.class.path.append=/home/pgrulich/projects/nes/nautilusbf/language/target/classes:/"
        //    "home/pgrulich/.m2/repository/com/googlecode/json-simple/json-simple/1.1/json-simple-1.1.jar",
        "-Djava.class.path=" + classpath};
    std::vector<JavaVMOption> options;
    for (const auto& s : opt) {
        options.push_back(JavaVMOption{.optionString = const_cast<char*>(s.c_str())});
    }
    args.version = JNI_VERSION_1_2;
    args.ignoreUnrecognized = false;
    args.options = options.data();
    args.nOptions = std::size(options);

    auto& instance = NES::Runtime::Execution::Operators::JVMContext::instance();
    instance.createOrAttachToJVM(&env, args);
    checkAndThrowException(env);

    jclass functionCallBytecodesClazz = env->FindClass("nebulastream/nodes/bytecodes/FunctionCallBytecodes");
    checkAndThrowException(env);

    static JNINativeMethod nativeMethods[] = {
        {const_cast<char*>("DyncallReset"), const_cast<char*>("()V"), (void*) &DyncallReset},
        {const_cast<char*>("DyncallSetArgB"), const_cast<char*>("(Z)V"), (void*) &DyncallSetArgB},
        {const_cast<char*>("DyncallSetArgI8"), const_cast<char*>("(B)V"), (void*) &DyncallSetArgI8},
        {const_cast<char*>("DyncallSetArgI16"), const_cast<char*>("(S)V"), (void*) &DyncallSetArgI16},
        {const_cast<char*>("DyncallSetArgI32"), const_cast<char*>("(I)V"), (void*) &DyncallSetArgI32},
        {const_cast<char*>("DyncallSetArgI64"), const_cast<char*>("(J)V"), (void*) &DyncallSetArgI64},
        {const_cast<char*>("DyncallSetArgPtr"), const_cast<char*>("(J)V"), (void*) &DyncallSetArgPtr},
        {const_cast<char*>("DyncallSetArgF"), const_cast<char*>("(F)V"), (void*) &DyncallSetArgF},
        {const_cast<char*>("DyncallSetArgD"), const_cast<char*>("(D)V"), (void*) &DyncallSetArgD},
        {const_cast<char*>("CallVoid"), const_cast<char*>("(J)V"), (void*) &CallVoid},
        {const_cast<char*>("CallI8"), const_cast<char*>("(J)B"), (void*) &CallI8},
        {const_cast<char*>("CallI16"), const_cast<char*>("(J)S"), (void*) &CallI16},
        {const_cast<char*>("CallI32"), const_cast<char*>("(J)I"), (void*) &CallI32},
        {const_cast<char*>("CallI64"), const_cast<char*>("(J)J"), (void*) &CallI64},
        {const_cast<char*>("CallFloat"), const_cast<char*>("(J)F"), (void*) &CallFloat},
        {const_cast<char*>("CallDouble"), const_cast<char*>("(J)D"), (void*) &CallDouble},
        {const_cast<char*>("CallPtr"), const_cast<char*>("(J)J"), (void*) &CallPtr},
    };

    env->RegisterNatives(functionCallBytecodesClazz, nativeMethods, sizeof(nativeMethods) / sizeof(JNINativeMethod));
    checkAndThrowException(env);

    jniEndpoint = env->FindClass("JNIEndpoint");
    checkAndThrowException(env);
}
BabelfishCompilationBackend::VM::~VM() {
    //Runtime::Execution::Operators::destroyVM();
}

std::unique_ptr<Executable>
BabelfishCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions&, const DumpHelper& dumpHelper) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto code = BabelfishLoweringProvider::lower(ir);
    dumpHelper.dump("babelfish.json", code.code);

    timer.snapshot("BabelfishIRGeneration");
    if (!vm) {
        vm = std::make_unique<VM>();
    } else {
      //  auto& instance = NES::Runtime::Execution::Operators::JVMContext::instance();
     //   instance.detachFromJVM();
     //   vm = std::make_unique<VM>();
    }

    auto initializePipeline =
        vm->env->GetStaticMethodID(vm->jniEndpoint, "initializePipeline", "(Ljava/lang/String;)Lnebulastream/Pipeline;");
    checkAndThrowException(vm->env);
    // Convert the std::string to a Java String
    jstring codeString = vm->env->NewStringUTF(code.code.c_str());
    checkAndThrowException(vm->env);
    // Call the Java load method with the Java String parameter
    auto pipeline = vm->env->CallStaticObjectMethod(vm->jniEndpoint, initializePipeline, codeString);
    checkAndThrowException(vm->env);
    // Release the local reference to the Java String
    vm->env->DeleteLocalRef(codeString);

    timer.snapshot("BabelfishPipelineCreation");
    NES_DEBUG(timer);

    return std::make_unique<BabelfishExecutable>(code, vm->env, vm->jniEndpoint, pipeline);
}

}// namespace NES::Nautilus::Backends::Babelfish