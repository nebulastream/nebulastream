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

#include <cstdint>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <UdfAbi.h>
#include <UdfBackend.hpp>

namespace NES
{

/// The entry points of one loaded copy of a bridge library.
struct BridgeLibrary
{
    void* handle = nullptr;
    void* initialize = nullptr; /// initialize_udf
    void* initializeFromSource = nullptr; /// initialize_udf_from_source, optional
    void* executeRow = nullptr; /// execute_udf_row
    void* executeBatch = nullptr; /// execute_udf_batch
    void* enterRow = nullptr; /// enter_udf_row
    void* exitRow = nullptr; /// exit_udf_row
    void* cleanup = nullptr; /// cleanup_udf
    /// The `free` of the bridge's C library, for the strings it hands over.
    void* free = nullptr;
    /// The optional stage functions: initialize_udf_stage, execute_udf_stage_row, execute_udf_stage_batch, cleanup_udf_stage.
    void* initializeStage = nullptr;
    void* executeStageRow = nullptr;
    void* executeStageBatch = nullptr;
    void* cleanupStage = nullptr;
};

/// In-process backend: loads the bridge `.so` and calls the UDF C ABI directly. With UdfInterpreter::DLMOPEN, every thread that
/// calls the UDF borrows a link namespace of its own for as long as it runs, with a copy of the bridge in it, and initializes the
/// UDF there unless an earlier thread with that namespace did.
/// Recoverable failures throw UdfExecutionError; a fatal crash in the `.so` takes down the worker.
class InProcessBackend final : public UdfBackend
{
public:
    /// What the bridge needs to initialize the UDF: an entry point, or `source` defining `functionName`.
    struct Definition
    {
        std::filesystem::path bridgePath;
        std::string entrypoint;
        std::string source;
        std::string functionName;
        std::vector<int> argTypeCodes;
        int returnTypeCode = 0;
    };

    /// Throws CannotLoadUdf if the bridge or the UDF cannot be loaded.
    InProcessBackend(Definition definition, UdfInterpreter interpreter);

    ~InProcessBackend() override;
    InProcessBackend(const InProcessBackend&) = delete;
    InProcessBackend& operator=(const InProcessBackend&) = delete;
    InProcessBackend(InProcessBackend&&) = delete;
    InProcessBackend& operator=(InProcessBackend&&) = delete;

    void executeScalarRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* resultScalar,
        int* resultNull) override;

    void executeVarsizedRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::vector<std::int8_t>& result,
        int* resultNull) override;

    void executeBatch(ArrowArray* arguments, ArrowArray* result) override;

    [[nodiscard]] int enterRow() override;
    void exitRow(int state) override;

    /// The copy of the bridge that serves the calling thread and the UDF's handle in it.
    [[nodiscard]] std::pair<const BridgeLibrary*, UdfInstance*> current();

    [[nodiscard]] UdfInterpreter getInterpreter() const { return interpreter; }

    [[nodiscard]] const std::vector<int>& getArgTypeCodes() const { return definition.argTypeCodes; }

    [[nodiscard]] int getReturnTypeCode() const { return definition.returnTypeCode; }

private:

    /// Initializes the UDF in `library`. Throws CannotLoadUdf on failure.
    [[nodiscard]] UdfInstance* initialize(const BridgeLibrary& library) const;

    /// Calls execute_udf_row, throwing UdfExecutionError on a recoverable failure. Sets `*resultString` to a string of the
    /// returned bridge, which the caller frees with that bridge's `free`.
    const BridgeLibrary& invokeUdf(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* resultScalar,
        char** resultString,
        long long* resultStringLen, /// NOLINT(google-runtime-int) the C ABI's type
        int* resultNull);

    Definition definition;
    UdfInterpreter interpreter;
    /// Identifies this backend in the per-thread handles of DLMOPEN, unlike its address, which a later backend may reuse.
    std::uint64_t id;
    /// The copy of the bridge and the handle for SHARED and SUBINTERPRETER.
    const BridgeLibrary* library = nullptr;
    UdfInstance* udfHandle = nullptr;

    /// For DLMOPEN: every copy of the bridge with the handle of the UDF in it, to clean them up.
    std::mutex handlesMutex;
    std::vector<std::pair<const BridgeLibrary*, UdfInstance*>> handles;
};

/// The UDFs of a UdfStage fused into one: one call into the bridge, and through it into the language, per row or batch for all UDFs,
/// see initialize_udf_stage. The UDFs share a bridge and an interpreter, and outlive this.
class InProcessFusedUdfs
{
public:
    /// Throws CannotLoadUdf if the bridge cannot fuse UDFs.
    explicit InProcessFusedUdfs(std::vector<std::shared_ptr<InProcessBackend>> udfs);
    ~InProcessFusedUdfs();
    InProcessFusedUdfs(const InProcessFusedUdfs&) = delete;
    InProcessFusedUdfs& operator=(const InProcessFusedUdfs&) = delete;
    InProcessFusedUdfs(InProcessFusedUdfs&&) = delete;
    InProcessFusedUdfs& operator=(InProcessFusedUdfs&&) = delete;

    /// A row in the layout of UdfStage::executeRow, except that the content of a VARSIZED result replaces that of
    /// `varsizedResults[udf]` instead of going to its slot. Throws UdfExecutionError on a recoverable failure of a UDF.
    void executeRow(
        const std::int8_t* argValues,
        const std::int8_t* argLens,
        const std::int8_t* argNulls,
        std::int8_t* results,
        std::int8_t* resultNulls,
        std::span<std::vector<std::int8_t>> varsizedResults);

    /// A batch, see execute_udf_stage_batch: takes ownership of `arguments` and, on success, sets `result`, which the caller then
    /// owns. Throws UdfExecutionError on a recoverable failure of a UDF.
    void executeBatch(ArrowArray* arguments, ArrowArray* result);

private:
    /// The copy of the bridge that serves the calling thread and the stage's handle in it.
    [[nodiscard]] std::pair<const BridgeLibrary*, UdfStageInstance*> current();
    [[nodiscard]] UdfStageInstance* initialize(const BridgeLibrary& library, const std::vector<UdfInstance*>& udfHandles) const;

    std::vector<std::shared_ptr<InProcessBackend>> udfs;
    std::size_t argumentCount = 0;
    std::uint64_t id;
    /// The copy of the bridge and the stage handle for SHARED and SUBINTERPRETER.
    const BridgeLibrary* library = nullptr;
    UdfStageInstance* stageHandle = nullptr;
    /// For DLMOPEN: every copy of the bridge with the stage handle in it, to clean them up.
    std::mutex handlesMutex;
    std::vector<std::pair<const BridgeLibrary*, UdfStageInstance*>> handles;
};

}
