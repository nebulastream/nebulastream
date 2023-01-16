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

#ifndef NES_EXECUTION_OPERATORS_RELATIONAL_JVMCONTEXT_HPP
#define NES_EXECUTION_OPERATORS_RELATIONAL_JVMCONTEXT_HPP

#include <mutex>
#include <jni.h>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief The JVMContext manages the JVM instance throughout the NebulaStream as a singleton.
 * This is needed because the creation of multiple VMs in a single process is not supported.
 * https://docs.oracle.com/javase/7/docs/technotes/guides/jni/spec/invocation.html#wp16334
 */
class JVMContext {
  public:
    static JVMContext& instance();

    /**
     * @brief The pointer to the JNI environment is only valid in the current thread. This call registers a thread.
     * First time calling leads to the creation of a JVM. Make sure to call detach before exiting.
     * @param env JNI environment to use for the thread
     * @param args initialization arguments needed when calling for the first time.
     * @return attach successful
     */
    bool createOrAttachToJVM(JNIEnv** env, JavaVMInitArgs &args);

    /**
     * @brief A thread attached to the VM must detach itself before exiting.
     * @return detach successful
     */
    bool detachFromJVM();

    /**
     * @brief Unloads a Java VM and reclaims its resources.
     * @return unloading successful
     */
    bool destroyJVM();

  private:
    JVMContext() {};
    JVMContext(JVMContext const&)      = delete;
    void operator=(JVMContext const&)  = delete;
    JVMContext(JVMContext const&&)      = delete;
    void operator=(JVMContext const&&)  = delete;

    bool attached = false;
    bool created = false;

    std::mutex mutex;
    JavaVM* jvm;
};

};// NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_OPERATORS_RELATIONAL_JVMCONTEXT_HPP