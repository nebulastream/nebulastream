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

namespace NES
{

/// Master switch for the "constant bytes are visible to the JIT" optimization, exposed as
/// `worker.query_engine.trace_constant_bytes` (default true = the previous hardcoded behavior).
/// It covers BOTH halves of the contribution, which only pays off when they land together:
///   - the LITERAL side (`nautilus::embedConstantBytes`): a host-known blob becomes a private
///     constant global in the JIT module instead of an opaque host pointer. Sites:
///     ConstantValueVariableSizePhysicalFunction (query string literals) and
///     OutputFormatterUtil::writeConstantBytes (CSV/JSON glue).
///   - the COMPARE side (`bytesEqual`): a call-free chunked compare loop instead of
///     `nautilus::memcmp`, so a compare against an embedded literal unrolls to immediate byte
///     compares. Sites: VARSIZED/UINT lazy eqImpl and VariableSizedData::operator==.
/// Turning it OFF restores the pre-4dddb90d2b/376053dc63 shape (opaque pointer + memcmp) WITHOUT
/// unpatching nautilus -- the `embedConstantBytes` API itself comes from the vcpkg port patch
/// (0002-add-constant-byte-globals.patch) and cannot be toggled at runtime, but its EFFECT can.
///
/// READ THIS ONLY FROM TRACE-TIME C++ (the call sites), never from inside a NAUTILUS_INLINE body:
/// those bodies are extracted to bitcode and linked into the JIT module, where touching a static
/// resolves to `__emutls_v.*` and the JIT dies with "Symbols not found". `bytesEqual` is
/// NAUTILUS_INLINE for exactly this reason -- branch at its callers, not inside it.
[[nodiscard]] bool traceConstantBytesEnabled();

/// Process-global, set once from the worker configuration before any query is traced. Follows the
/// InvokeConfig::instance() precedent (trace-time knobs are global, not per-request).
void setTraceConstantBytes(bool enabled);

}
