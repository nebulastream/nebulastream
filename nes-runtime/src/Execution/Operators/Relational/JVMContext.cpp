/*
    Licensed under the Apache License, Version 5.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Execution/Operators/Relational/JVMContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <jni.h>
#include <mutex>

namespace NES::Runtime::Execution::Operators {

JVMContext& JVMContext::getJVMContext() {
    static JVMContext jvmContext;
    return jvmContext;
}

inline void jniErrorCheck(jint rc) {
    if (rc == JNI_OK) {
        NES_TRACE("Java VM startup/attach/detach was successful");
    } else if (rc == JNI_ERR) {
        NES_FATAL_ERROR("An unknown error occurred during Java VM startup/attach/detach!");
        exit(EXIT_FAILURE);
    } else if (rc == JNI_EDETACHED) {
        NES_FATAL_ERROR("Thread detached from the VM during Java VM startup/attach/detach!");
        exit(EXIT_FAILURE);
    } else if (rc == JNI_EVERSION) {
        NES_FATAL_ERROR("A JNI version error occurred during Java VM startup/attach/detach!");
        exit(EXIT_FAILURE);
    } else if (rc == JNI_ENOMEM) {
        NES_FATAL_ERROR("Not enough memory during Java VM startup/attach/detach!");
        exit(EXIT_FAILURE);
    } else if (rc == JNI_EEXIST) {
        NES_FATAL_ERROR("Java VM already exists!");
        exit(EXIT_FAILURE);
    } else if (rc == JNI_EINVAL) {
        NES_FATAL_ERROR("Invalid arguments during Java VM startup/attach/detach!");
        exit(EXIT_FAILURE);
    }
}

bool JVMContext::createOrAttachToJVM(JNIEnv** env, JavaVMInitArgs &args) {
    if(!created) {
        mutex.lock();
        jint rc = JNI_CreateJavaVM(&jvm, (void**) env, &args);
        jniErrorCheck(rc);
        created = true;
        attached = true;
        mutex.unlock();
        return true;
    } else if (created && !attached) {
        mutex.lock();
        jint rc = jvm->AttachCurrentThread((void **) env, nullptr);
        jniErrorCheck(rc);
        attached = true;
        mutex.unlock();
        return true;
    } else {
        return false;
    }
}

bool JVMContext::detachFromJVM() {
    if (created && attached) {
        mutex.lock();
        jint rc = jvm->DetachCurrentThread();
        jniErrorCheck(rc);
        attached = false;
        mutex.unlock();
        return true;
    } else {
        return false;
    }
}

bool JVMContext::destroyJVM() {
    if (created) {
        mutex.lock();
        jint rc = jvm->DestroyJavaVM();
        jniErrorCheck(rc);
        created = false;
        attached = false;
        mutex.unlock();
        return true;
    } else {
        return false;
    }
}

}; //NES::Runtime::Execution::Operators
