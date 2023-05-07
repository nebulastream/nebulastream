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

#include <Nautilus/Backends/Babelfish/BabelfishCode.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishCompilationBackend.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishExecutable.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishLoweringProvider.hpp>
#include <Nautilus/Util/Dyncall.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <jni.h>
#include <string>

namespace NES::Nautilus::Backends::Babelfish {

[[maybe_unused]] static CompilationBackendRegistry::Add<BabelfishCompilationBackend> babelfishBackend("BabelfishCompiler");

JNIEXPORT void JNICALL DyncallReset(JNIEnv*, jclass) { BC::Dyncall::getVM().reset(); }
JNIEXPORT void JNICALL DyncallSetArgB(JNIEnv*, jclass, jboolean value) { BC::Dyncall::getVM().addArgB(value); }
JNIEXPORT void JNICALL DyncallSetArgI8(JNIEnv*, jclass, jbyte value) { BC::Dyncall::getVM().addArgI8(value); }
JNIEXPORT void JNICALL DyncallSetArgI16(JNIEnv*, jclass, jshort value) { BC::Dyncall::getVM().addArgI16(value); }
JNIEXPORT void JNICALL DyncallSetArgI32(JNIEnv*, jclass, jint value) { BC::Dyncall::getVM().addArgI32(value); }
JNIEXPORT void JNICALL DyncallSetArgI64(JNIEnv*, jclass, jlong value) { BC::Dyncall::getVM().addArgI64(value); }
JNIEXPORT void JNICALL DyncallSetArgF(JNIEnv*, jclass, jfloat value) { BC::Dyncall::getVM().addArgF(value); }
JNIEXPORT void JNICALL DyncallSetArgD(JNIEnv*, jclass, jdouble value) { BC::Dyncall::getVM().addArgD(value); }

JNIEXPORT void JNICALL DyncallSetArgPtr(JNIEnv*, jclass, jlong value) {
    auto ptr = (void*) value;
    BC::Dyncall::getVM().addArgPtr(ptr);
}

JNIEXPORT void JNICALL DyncallSetArgObject(JNIEnv* env, jclass, jobject value) {
    auto global = env->NewGlobalRef(value);
    BC::Dyncall::getVM().addArgPtr(global);
}

JNIEXPORT void JNICALL CallVoid(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    BC::Dyncall::getVM().callVoid(address);
}
JNIEXPORT jbyte JNICALL CallI8(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI8(address);
}
JNIEXPORT jshort JNICALL CallI16(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI16(address);
}
JNIEXPORT jint JNICALL CallI32(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI32(address);
}
JNIEXPORT jlong JNICALL CallI64(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callI64(address);
}
JNIEXPORT jfloat JNICALL CallFloat(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callF(address);
}
JNIEXPORT jdouble JNICALL CallDouble(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    return BC::Dyncall::getVM().callD(address);
}

JNIEXPORT jlong JNICALL CallPtr(JNIEnv*, jclass, jlong fptr) {
    auto address = (void*) fptr;
    auto resultPtr = BC::Dyncall::getVM().callPtr(address);
    return (int64_t) resultPtr;
}

JNIEXPORT jobject JNICALL GetObject(JNIEnv*, jclass, jlong ptr) { return (jobject) ptr; }

BabelfishCompilationBackend::BabelfishCompilationBackend() {
    std::string classpath = std::string(BABELFISH_PATH) + "/launcher/target/launcher.jar";
    auto& jvm = jni::JVM::get();
    // jvm.addOption("-verbose:class");
    // jvm.addOption("-verbose:jni");
    jvm.addOption("-Dgraal.Dump=Truffle:1");
    jvm.addOption("-Dgraal.PrintGraph=Network");
    jvm.addOption("-Dgraal.GenLoopSafepoints=false");
    jvm.addOption("-Dgraalvm.locatorDisabled=true");
    jvm.addClasspath(classpath);
}

BabelfishCompilationBackend::BabelfishEngine::BabelfishEngine() {
    auto& jvm = jni::JVM::get();
    if (!jvm.isInitialized()) {
        jvm.init();
    }
    auto env = jni::getEnv();
    jclass functionCallBytecodesClazz = env->FindClass("nebulastream/nodes/bytecodes/FunctionCallBytecodes");
    jni::jniErrorCheck();

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
        {const_cast<char*>("DyncallSetArgObject"), const_cast<char*>("(Ljava/lang/Object;)V"), (void*) &DyncallSetArgObject},
        {const_cast<char*>("CallVoid"), const_cast<char*>("(J)V"), (void*) &CallVoid},
        {const_cast<char*>("CallI8"), const_cast<char*>("(J)B"), (void*) &CallI8},
        {const_cast<char*>("CallI16"), const_cast<char*>("(J)S"), (void*) &CallI16},
        {const_cast<char*>("CallI32"), const_cast<char*>("(J)I"), (void*) &CallI32},
        {const_cast<char*>("CallI64"), const_cast<char*>("(J)J"), (void*) &CallI64},
        {const_cast<char*>("CallFloat"), const_cast<char*>("(J)F"), (void*) &CallFloat},
        {const_cast<char*>("CallDouble"), const_cast<char*>("(J)D"), (void*) &CallDouble},
        {const_cast<char*>("CallPtr"), const_cast<char*>("(J)J"), (void*) &CallPtr},
        {const_cast<char*>("GetObject"), const_cast<char*>("(J)Ljava/lang/Object;"), (void*) &GetObject},
    };

    env->RegisterNatives(functionCallBytecodesClazz, nativeMethods, sizeof(nativeMethods) / sizeof(JNINativeMethod));
    jni::jniErrorCheck();

    jniEndpoint = jni::findClass("JNIEndpoint");
}
BabelfishCompilationBackend::BabelfishEngine::~BabelfishEngine() { jni::getEnv()->DeleteGlobalRef(jniEndpoint); }

std::unique_ptr<Executable>
BabelfishCompilationBackend::BabelfishEngine::deploy(const NES::Nautilus::Backends::Babelfish::BabelfishCode& code) {
    auto env = jni::getEnv();
    auto initializePipeline =
        env->GetStaticMethodID(jniEndpoint, "initializePipeline", "(Ljava/lang/String;)Lnebulastream/Pipeline;");
    jni::jniErrorCheck();
    // Convert the std::string to a Java String
    jstring codeString = env->NewStringUTF(code.code.c_str());
    jni::jniErrorCheck();
    // Call the Java load method with the Java String parameter
    auto pipeline = env->CallStaticObjectMethod(jniEndpoint, initializePipeline, codeString);
    jni::jniErrorCheck();
    // Release the local reference to the Java String
    env->DeleteLocalRef(codeString);
    auto globalPipeline = env->NewGlobalRef(pipeline);
    jni::detatchEnv();
    return std::make_unique<BabelfishExecutable>(code, jniEndpoint, globalPipeline);
}

std::unique_ptr<Executable>
BabelfishCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions&, const DumpHelper& dumpHelper) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    // lower nautilus to babelfish json input
    auto code = BabelfishLoweringProvider::lower(ir);
    dumpHelper.dump("babelfish.json", code.code);

    timer.snapshot("BabelfishIRGeneration");

    // initialize engine if not already running.
    if (!engine) {
        engine = std::make_unique<BabelfishEngine>();
    }
    // compile code and deploy to engine.
    auto executable = engine->deploy(code);

    timer.snapshot("BabelfishPipelineCreation");
    NES_DEBUG(timer);
    return executable;
}

}// namespace NES::Nautilus::Backends::Babelfish