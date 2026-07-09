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

/// The NebulaStream scalar-UDF C ABI.
///
/// A registered UDF `.so` exposes exactly the three symbols declared below.
/// The engine never calls the UDF author's code directly — it calls this ABI,
/// which a language bridge (e.g. an embedded Python interpreter) implements.
/// The ABI is therefore language-agnostic: any `.so` that exports these three
/// symbols can serve scalar UDFs.
///
/// Contract highlights (see docs/design/20260708_Scalar_UDF_Support.md):
///   - NULL is carried by explicit per-value flags, never an in-band sentinel.
///   - VARSIZED values are length-delimited (binary-safe), never NUL-terminated.
///   - A recoverable failure returns UDF_ERROR with a malloc'd *errormessage
///     that the caller frees; the engine turns it into a query error and the
///     worker survives. A fatal fault (segfault / abort) is NOT contained by
///     this ABI — that is the job of the (future) out-of-process backend.

#ifdef __cplusplus
extern "C"
{
#endif

/// Wire type codes for UDF argument and return values. Stable integer values —
/// they cross the ABI boundary — chosen to line up with NES::DataType::Type.
enum UdfTypeCode
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
    UDF_VARSIZED = 11
};

/// Return status of execute_udf_row. NULL-ness is a separate out-parameter flag,
/// not a status value.
enum UdfStatus
{
    UDF_ERROR = 0, /// the UDF failed recoverably; *errormessage is set (caller frees it)
    UDF_OK = 1     /// success; the result_* out-params (and result_null) are written
};

/// Prepare a UDF for execution. `entrypoint` identifies the function inside the
/// `.so` (for the Python bridge, a "module.function" string). `arg_type_codes`
/// has `argc` entries; `return_type_code` is a single UdfTypeCode. Returns a
/// non-negative handle on success, or a negative value with *errormessage set.
int initialize_udf(
    const char* entrypoint, int argc, const int* arg_type_codes, int return_type_code, char** errormessage);

/// Execute the UDF for a single row.
///   arg_values[i]      -> the i-th argument value; for UDF_VARSIZED it points at
///                         the raw bytes (see arg_lens[i]).
///   arg_lens[i]        -> byte length of a UDF_VARSIZED argument; ignored (0) for scalars.
///   arg_nulls[i]       -> non-zero => the i-th argument is SQL NULL (value ignored).
///   result_scalar      -> 8-byte slot; the result is written here for numeric/bool
///                         returns, reinterpreted per return_type_code.
///   result_string      -> for a VARSIZED return, set to a malloc'd buffer (caller frees);
///   result_string_len  -> its byte length.
///   result_null        -> set non-zero if the UDF returned NULL.
///   errormessage       -> on UDF_ERROR, a malloc'd message (caller frees).
/// Returns a UdfStatus.
int execute_udf_row(
    int handle,
    const void* const* arg_values,
    const long long* arg_lens,
    const int* arg_nulls,
    void* result_scalar,
    char** result_string,
    long long* result_string_len,
    int* result_null,
    char** errormessage);

/// Release all resources associated with a handle returned by initialize_udf.
void cleanup_udf(int handle);

#ifdef __cplusplus
}
#endif
