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

#include <Util/JNI/JNI.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>
#include <jni.h>
#include <string>
#include <string_view>

/* The following code is inspired by jnipp provided by @mitchdowd (https://github.com/mitchdowd/jnipp)
MIT License
Copyright (c) 2016 Mitchell Dowd

Permission is hereby granted, free of charge, to any person obtaining a copy
                                                  of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

       THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
        */

namespace NES::jni {

// Static Variables
static std::atomic_bool isJVMInitialized(false);
static JavaVM* jvmInstance = nullptr;

static bool getEnv(JavaVM* vm, JNIEnv** env) {
    NES_ASSERT(vm != nullptr, "java vm should not be null");
    return vm->GetEnv((void**) env, JNI_VERSION_1_8) == JNI_OK;
}

static bool isAttached(JavaVM* vm) {
    JNIEnv* env = nullptr;
    // we can assume that get env will not return JNI_EVERSION
    return getEnv(vm, &env);
}

/**
* Maintains the lifecycle of a JNIEnv in a thread.
*/
class ScopedEnv final {
  public:
    ScopedEnv() noexcept : vm(nullptr), env(nullptr), attached(false) {}
    ~ScopedEnv();

    void init(JavaVM* vm);
    JNIEnv* get() const noexcept { return env; }

  private:
    JavaVM* vm;
    JNIEnv* env;
    bool attached;///< Manually attached, as opposed to already attached.
};

ScopedEnv::~ScopedEnv() {
    if (vm && attached) {
        vm->DetachCurrentThread();
    }
}

void ScopedEnv::init(JavaVM* vm) {
    if (env != nullptr) {
        return;
    }

    if (vm == nullptr) {
        throw InitializationException("JNI not initialized");
    }
    if (!getEnv(vm, &env)) {
        if (vm->AttachCurrentThread((void**) &env, nullptr) != 0) {
            throw InitializationException("Could not attach JNI to thread");
        }
        attached = true;
    }

    this->vm = vm;
}

JNIEnv* getEnv() {
    static thread_local ScopedEnv env;

    if (env.get() != nullptr && !isAttached(jvmInstance)) {
        // we got detached, so clear it.
        // will be re-populated from static javaVm below.
        env = ScopedEnv{};
    }

    if (env.get() == nullptr) {
        env.init(jvmInstance);
    }

    return env.get();
}

void detachEnv() {
    if (jvmInstance == nullptr) {
        throw InitializationException("JNI not initialized");
    }
    jvmInstance->DetachCurrentThread();
}

JVM& JVM::get() {
    static JVM jvm = {};
    return jvm;
}

void JVM::init() {
    bool expected = false;
    if (!isJVMInitialized.compare_exchange_strong(expected, true)) {
        throw InitializationException("Java Virtual Machine already initialized");
    }

    if (jvmInstance == nullptr) {
        JavaVMInitArgs args{};
        std::vector<JavaVMOption> jvmOptions;
        for (const auto& option : options) {
            NES_DEBUG("JVM add option {}", option);
            jvmOptions.push_back(JavaVMOption{.optionString = const_cast<char*>(option.c_str())});
        }

        if (!classPath.empty()) {
            NES_DEBUG("JVM add class path {}", classPath);
            jvmOptions.push_back(JavaVMOption{.optionString = const_cast<char*>(classPath.c_str())});
        }
        args.version = JNI_VERSION_1_8;
        args.ignoreUnrecognized = false;
        args.options = jvmOptions.data();
        args.nOptions = std::size(jvmOptions);

        JNIEnv* env;
        if (JNI_CreateJavaVM(&jvmInstance, (void**) &env, &args) != JNI_OK) {
            isJVMInitialized.store(false);
            throw InitializationException("Java Virtual Machine failed during creation");
        }
    }
}

JVM& JVM::addClasspath(const std::string& path) {
    if (isInitialized()) {
        throw InitializationException(
            "ClassPath can't be changed to running jvm. All classpaths have been provided before JVM init is called.");
    }
    std::string prefix = classPath.empty() ? "-Djava.class.path=" : ":";
    classPath = classPath + prefix + path;
    return *this;
}

JVM& JVM::addOption(const std::string& option) {
    if (isInitialized()) {
        throw InitializationException(
            "Options can't be attached to running jvm. All options have been provided before JVM init is called.");
    }
    options.emplace_back(option);
    return *this;
}
bool JVM::isInitialized() { return isJVMInitialized; }

}// namespace NES::jni