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
#ifdef ENABLE_JNI

#include <Execution/Operators/Relational/JVMContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <jni.h>
#include <mutex>

namespace NES::Runtime::Execution::Operators {

JVMContext& JVMContext::instance() {
    static JVMContext jvmContext;
    return jvmContext;
}

inline void jniErrorCheck(jint rc) {
    if (rc == JNI_OK) {
        NES_INFO("Java VM startup/attach/detach was successful");
    } else if (rc == JNI_ERR) {
        NES_THROW_RUNTIME_ERROR("An unknown error occurred during Java VM startup/attach/detach!");
    } else if (rc == JNI_EDETACHED) {
        NES_THROW_RUNTIME_ERROR("Thread detached from the VM during Java VM startup/attach/detach!");
    } else if (rc == JNI_EVERSION) {
        NES_THROW_RUNTIME_ERROR("A JNI version error occurred during Java VM startup/attach/detach!");
    } else if (rc == JNI_ENOMEM) {
        NES_THROW_RUNTIME_ERROR("Not enough memory during Java VM startup/attach/detach!");
    } else if (rc == JNI_EEXIST) {
        NES_THROW_RUNTIME_ERROR("Java VM already exists!");
    } else if (rc == JNI_EINVAL) {
        NES_THROW_RUNTIME_ERROR("Invalid arguments during Java VM startup/attach/detach!");
    }
}

void JVMContext::createOrAttachToJVM(JNIEnv** env, JavaVMInitArgs &args) {
    std::lock_guard<std::mutex> lock(mutex);
    if(!created) {
        jint rc = JNI_CreateJavaVM(&jvm, (void**) env, &args);
        jniErrorCheck(rc);
        created = true;
        attached = true;
    } else if (created && !attached) {
        jint rc = jvm->AttachCurrentThread((void**) env, nullptr);
        jniErrorCheck(rc);
        attached = true;
    } else {
        NES_THROW_RUNTIME_ERROR("Try to attach to a JVM that is already attached!");
    }
}

void JVMContext::detachFromJVM() {
    std::lock_guard<std::mutex> lock(mutex);
    if (created && attached) {
        jint rc = jvm->DetachCurrentThread();
        jniErrorCheck(rc);
        attached = false;
    } else {
        NES_THROW_RUNTIME_ERROR("Try to detach from a JVM that is not attached!");
    }
}

void JVMContext::destroyJVM() {
    std::lock_guard<std::mutex> lock(mutex);
    if (created) {
        jint rc = jvm->DestroyJavaVM();
        jniErrorCheck(rc);
        created = false;
        attached = false;
    } else {
        NES_THROW_RUNTIME_ERROR("Try to destroy a JVM that is not created!");
    }
}

}; //NES::Runtime::Execution::Operators
#endif //ENABLE_JNI
