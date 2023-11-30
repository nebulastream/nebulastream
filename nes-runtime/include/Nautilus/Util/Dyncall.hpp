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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_DYNCALL_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_DYNCALL_HPP_

#include <cinttypes>
#include <cstdint>

typedef struct DCCallVM_ DCCallVM;
namespace NES::Nautilus::Backends::BC {

/**
 * @class Dyncall
 * @brief A C++ wrapper for the dyncall C library.
 *
 * The Dyncall class is a C++ wrapper for the dyncall C library, providing an object-oriented interface
 * for calling C functions at runtime. This allows for easier integration and usage of the dyncall library
 * within C++ projects.
 *
 * Example usage:
 * @code
 * Dyncall& dyncall = Dyncall::getVM();
 * dyncall.reset();
 * dyncall.addArgI32(42);
 * int32_t result = dyncall.callI32(someFunctionPointer);
 * @endcode
 */
class Dyncall {
  public:
    /**
   * @brief Default stack size for the Dyncall virtual machine (VM).
   */
    static constexpr uint64_t VM_STACK_SIZE = 4096;

    /**
   * @brief Default constructor.
   */
    Dyncall();

    /**
   * @brief Singleton instance access for the Dyncall VM.
   * @return A reference to the singleton Dyncall VM instance.
   */
    static Dyncall& getVM();

    /**
   * @brief Resets the VM state, clearing all previously added arguments.
   */
    void reset();

    /**
   * @brief Add a boolean argument to the VM.
   * @param value The boolean value to add.
   */
    void addArgB(bool value);
    /**
   * @brief Add an int8_t argument to the VM.
   * @param value The int8_t value to add.
   */
    void addArgI8(int8_t value);

    /**
   * @brief Add an int16_t argument to the VM.
   * @param value The int16_t value to add.
   */
    void addArgI16(int16_t value);

    /**
   * @brief Add an int32_t argument to the VM.
   * @param value The int32_t value to add.
   */
    void addArgI32(int32_t value);

    /**
   * @brief Add an int64_t argument to the VM.
   * @param value The int64_t value to add.
   */
    void addArgI64(int64_t value);

    /**
   * @brief Add a double argument to the VM.
   * @param value The double value to add.
   */
    void addArgD(double value);

    /**
   * @brief Add a float argument to the VM.
   * @param value The float value to add.
   */
    void addArgF(float value);

    /**
   * @brief Add a pointer argument to the VM.
   * @param value The pointer value to add.
   */
    void addArgPtr(void* value);

    /**
    * @brief Call a void function with the provided function pointer and the currently set arguments.
    * @param value Function pointer to the target void function.
    */
    void callVoid(void* value);

    /**
    * @brief Call a function returning a boolean value with the provided function pointer and the currently set arguments.
    * @param value Function pointer to the target function returning a boolean value.
    * @return The boolean result of the function call.
    */
    bool callB(void* value);

    /**
   * @brief Call a function returning an int8_t value with the provided function pointer and the currently set arguments.
   * @param value Function pointer to the target function returning an int8_t value.
   * @return The int8_t result of the function call.
   */
    int8_t callI8(void* value);

    /**
   * @brief Call a function returning an int16_t value with the provided function pointer and the currently set arguments.
   * @param value Function pointer to the target function returning an int16_t value.
   * @return The int16_t result of the function call.
   */
    int16_t callI16(void* value);

    /**
   * @brief Call a function returning an int32_t value with the provided function pointer and the currently set arguments.
   * @param value Function pointer to the target function returning an int32_t value.
   * @return The int32_t result of the function call.
   */
    int32_t callI32(void* value);

    /**
   * @brief Call a function returning an int64_t value with the provided function pointer and the currently set arguments.
   * @param value Function pointer to the target function returning an int64_t value.
   * @return The int64_t result of the function call.
   */
    int64_t callI64(void* value);

    /**
   * @brief Call a function returning an double value with the provided function pointer and the currently set arguments.
   * @param value Function pointer to the target function returning an double value.
   * @return The double result of the function call.
   */
    double callD(void* value);

    /**
    @brief Call a function returning a float value with the provided function pointer and the currently set arguments.
    @param value Function pointer to the target function returning a float value.
    @return The float result of the function call.
    */
    float callF(void* value);

    /**
   * @brief Call a function returning an void* value with the provided function pointer and the currently set arguments.
   * @param value Function pointer to the target function returning an void* value.
   * @return The void* result of the function call.
   */
    void* callPtr(void* value);

  private:
    /**
   * @brief Pointer to the underlying dyncall VM instance.
   */
    DCCallVM* vm;
};
}// namespace NES::Nautilus::Backends::BC

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_DYNCALL_HPP_
