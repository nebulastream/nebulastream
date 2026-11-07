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

#pragma once

/// C ABI shared with the UDF bridges: plain C enums and the C integer types the bridges declare.
/// NOLINTBEGIN(cppcoreguidelines-use-enum-class,google-runtime-int)

/// The NebulaStream scalar-UDF C ABI: a registered UDF `.so` exposes the symbols below, some of which are optional,
/// implemented by a language bridge (e.g. an embedded Python interpreter). Any `.so` exporting them can serve scalar UDFs.
///
/// Contract highlights (see docs/design/20260708_Scalar_UDF_Support.md):
///   - UDFs run per row or on batches of rows in Arrow's columnar format. NULL is an explicit flag or validity bit, never an
///     in-band sentinel, and VARSIZED values are length-delimited (binary-safe), never NUL-terminated.
///   - Strings the bridge hands over are malloc'd with the bridge's C library and freed with its `free`, which is not the
///     caller's if the caller loaded the bridge into a link namespace of its own (dlmopen).
///   - A recoverable failure returns UDF_ERROR with a malloc'd *errormessage (caller frees); a fatal
///     fault (segfault/abort) is NOT contained here -- that's the job of the future out-of-process backend.

#include <stdint.h> /// NOLINT(modernize-deprecated-headers) a C header

#ifdef __cplusplus
extern "C" {
#endif

/// The Arrow C Data Interface, copied verbatim as its specification asks, so that it is defined once whichever Arrow
/// implementation's headers come first. https://arrow.apache.org/docs/format/CDataInterface.html
/// NOLINTBEGIN
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE
/// NOLINTEND

/// Wire type codes for UDF argument and return values. Stable integer values —
/// they cross the ABI boundary — chosen to line up with NES::DataType::Type.
enum UdfTypeCode /// NOLINT(performance-enum-size) C has no fixed underlying enum types before C23
{
    UDF_BOOL = 0,
    UDF_INT8 = 1,
    UDF_INT16 = 2,
    UDF_INT32 = 3,
    UDF_INT64 = 4,
    UDF_UINT8 = 5,
    UDF_UINT16 = 6,
    UDF_UINT32 = 7,
    UDF_UINT64 = 8,
    UDF_FLOAT32 = 9,
    UDF_FLOAT64 = 10,
    UDF_VARSIZED = 11,
    UDF_CHAR = 12 /// a single byte; a bridge offers it like a one-byte VARSIZED value
};

/// Return status of execute_udf_row and execute_udf_batch. NULL-ness of a row's result is a separate out-parameter flag.
enum UdfStatus /// NOLINT(performance-enum-size) C has no fixed underlying enum types before C23
{
    UDF_ERROR = 0, /// the UDF failed recoverably; *errormessage is set (caller frees it)
    UDF_OK = 1 /// success; the result is written
};

/// The ABI is plain C, so its parameters use C naming.
/// NOLINTBEGIN(readability-identifier-naming)

/// An initialized UDF, and UDFs combined into a stage. The bridge defines them; the caller only passes them back. A handle is valid
/// from its initialization until its cleanup: calls with it may run in parallel on several threads, but none after or during its
/// cleanup. Passing a handle needs no lookup in the bridge, so that calls on different threads share no state there.
typedef struct UdfInstance UdfInstance;
typedef struct UdfStageInstance UdfStageInstance;

/// Prepare a UDF for execution. `entrypoint` identifies the function inside the
/// `.so` (for the Python bridge, a "module.function" string). `arg_type_codes`
/// has `argc` entries; `return_type_code` is a single UdfTypeCode. Returns the
/// UDF on success, or NULL with *errormessage set.
UdfInstance* initialize_udf(const char* entrypoint, int argc, const int* arg_type_codes, int return_type_code, char** errormessage);

/// Prepare a UDF for execution from literal source rather than an importable module. `source` is
/// executed in a fresh, anonymous namespace (private to this one handle); `function_name` names the
/// callable it defines. Used for UDFs defined with AS in CREATE FUNCTION, which carry their
/// body as text instead of a file reachable via the bridge's module search path. Same argument,
/// return, and handle contract as initialize_udf. Optional: a bridge that never serves inline UDFs may
/// omit it.
UdfInstance* initialize_udf_from_source(
    const char* source, const char* function_name, int argc, const int* arg_type_codes, int return_type_code, char** errormessage);

/// Execute the UDF for a single row.
///   arg_values[i]      -> the i-th argument value; for UDF_VARSIZED it points at
///                         the raw bytes (see arg_lens[i]).
///   arg_lens[i]        -> byte length of a UDF_VARSIZED argument; ignored (0) for scalars.
///   arg_nulls[i]       -> non-zero => the i-th argument is SQL NULL (value ignored).
///   result_scalar      -> 8-byte slot; the result is written here for numeric/bool/char
///                         returns, reinterpreted per return_type_code.
///   result_string      -> for a VARSIZED return, set to a malloc'd buffer (caller frees);
///   result_string_len  -> its byte length.
///   result_null        -> set non-zero if the UDF returned NULL.
///   errormessage       -> on UDF_ERROR, a malloc'd message (caller frees).
/// Returns a UdfStatus.
int execute_udf_row(
    UdfInstance* udf,
    const void* const* arg_values,
    const long long* arg_lens,
    const int* arg_nulls,
    void* result_scalar,
    char** result_string,
    long long* result_string_len,
    int* result_null,
    char** errormessage);

/// Execute the UDF on a batch of rows, handed over through the Arrow C Data Interface.
///   arguments    -> a struct array with one nullable child per argument, of the Arrow type of its type code: bool, the
///                   (unsigned) integer of its width, float, double, binary for UDF_VARSIZED and fixed_size_binary(1) for
///                   UDF_CHAR. The bridge takes ownership and releases it; it must not read the buffers after returning.
///   result       -> on UDF_OK, set to an array of as many rows, of the Arrow type of the return type code, NULL where the UDF
///                   returned NULL. The caller takes ownership and releases it.
///   errormessage -> on UDF_ERROR, a malloc'd message (caller frees).
/// Returns a UdfStatus.
int execute_udf_batch(UdfInstance* udf, struct ArrowArray* arguments, struct ArrowArray* result, char** errormessage);

/// Combine `count` UDFs of this `.so` into a stage that runs them together: one call into the language per row or batch for all
/// of them, instead of one per UDF. Optional, with the three functions below. Returns the stage on success, or NULL with
/// *errormessage set. Clean the stage up before its UDFs.
UdfStageInstance* initialize_udf_stage(int count, UdfInstance* const* udfs, char** errormessage);

/// execute_udf_row for all UDFs of a stage at once. The arguments are those of all UDFs one after the other. For the i-th UDF:
///   result_scalars     -> the i-th of `count` 8-byte slots, written for a numeric/bool/char return.
///   result_strings[i]  -> for a VARSIZED return, set to a malloc'd buffer (caller frees), of result_string_lens[i] bytes.
///   result_nulls[i]    -> set non-zero if the UDF returned NULL.
/// On UDF_ERROR, no result string is left for the caller to free.
int execute_udf_stage_row(
    UdfStageInstance* stage,
    const void* const* arg_values,
    const long long* arg_lens,
    const int* arg_nulls,
    void* result_scalars,
    char** result_strings,
    long long* result_string_lens,
    int* result_nulls,
    char** errormessage);

/// execute_udf_batch for all UDFs of a stage at once. `arguments` is a struct array of the argument columns of all UDFs one after
/// the other; on UDF_OK, `result` is set to a struct array with one child per UDF, its result column.
int execute_udf_stage_batch(UdfStageInstance* stage, struct ArrowArray* arguments, struct ArrowArray* result, char** errormessage);

/// Release a stage. Its UDFs stay initialized.
void cleanup_udf_stage(UdfStageInstance* stage);

/// Bracket the execute_udf_row or execute_udf_batch calls that the caller makes for one row or batch, so that the bridge takes
/// what it serializes on (the GIL for CPython) once instead of per call. Only valid after initializing a UDF of this `.so`.
/// A non-zero `isolated` runs the calls in an interpreter of the calling thread's own (a subinterpreter with its own GIL for
/// CPython), which the bridge keeps for the thread. exit_udf_row takes the value that enter_udf_row returned, on the same thread.
int enter_udf_row(int isolated);
void exit_udf_row(int state);

/// Release all resources of a UDF that initialize_udf returned.
void cleanup_udf(UdfInstance* udf);

/// NOLINTEND(readability-identifier-naming)

#ifdef __cplusplus
}
#endif
/// NOLINTEND(cppcoreguidelines-use-enum-class,google-runtime-int)
