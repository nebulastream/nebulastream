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

#include <filesystem>
#include <memory>
#include <optional>
#include <string>

namespace NES
{
class SystestConfiguration;
}

namespace NES::Systest
{

/// Read the single `# requires:` directive from a .test file's header. Lets
/// the dispatcher decide whether to act before the full systest parser fires.
/// At most one directive is allowed; throws TestException on duplicates.
std::optional<std::string> readRequirementFromHeader(const std::filesystem::path& testFile);

/// Forward — defined in the .cpp because callers don't need its internals,
/// only the dispatch guard's lifetime ownership of it.
class ProfileVolumeGuard;

/// RAII guard owning the lifecycle of a docker-compose stack the dispatcher
/// brought up for an external systest, plus the docker volume that supplies
/// the broker container with the plugin's profile-dir contents (config files,
/// fixtures). Construction stages the volume + runs `docker compose up`;
/// destruction tears the compose stack down + removes the volume.
///
/// Held by main() across the executor's run so the broker stays alive for the
/// duration of the test; destructor fires on normal return and on exception
/// unwinds. The volume-based approach (vs a host bind mount) means we don't
/// need to know the dev-container-vs-host path mapping (no
/// HOST_NEBULASTREAM_ROOT dependency) — the daemon manages the volume
/// regardless of where on the host filesystem it's actually backed.
class ExternalSystestDispatchGuard
{
public:
    ExternalSystestDispatchGuard(std::string projectName, std::filesystem::path composeFile, std::unique_ptr<ProfileVolumeGuard> volume);
    ~ExternalSystestDispatchGuard();

    ExternalSystestDispatchGuard(const ExternalSystestDispatchGuard&) = delete;
    ExternalSystestDispatchGuard& operator=(const ExternalSystestDispatchGuard&) = delete;
    ExternalSystestDispatchGuard(ExternalSystestDispatchGuard&&) = delete;
    ExternalSystestDispatchGuard& operator=(ExternalSystestDispatchGuard&&) = delete;

private:
    std::string projectName;
    std::filesystem::path composeFile;
    /// Destroyed *after* the compose stack comes down — volume must outlive
    /// any container that mounts it. Declaration order matters: members are
    /// destroyed in reverse order, and `volume` is below `projectName` /
    /// `composeFile` so it is destroyed *first* unless we shuffle. We move
    /// it to a `unique_ptr` declared last so destruction order is correct:
    /// compose-down happens in the explicit destructor body before this
    /// member's destructor runs.
    std::unique_ptr<ProfileVolumeGuard> volume;
    bool up = false;
};

/// If the configured test file declares `# requires:` and the caller hasn't
/// already satisfied it via --accept-requires, bring up the broker docker
/// stack, rewrite the relevant source-config knobs so the in-process test
/// reaches the broker on a host port, mark `--accept-requires` satisfied in
/// `config`, and return a guard that tears the stack down on destruction.
///
/// Returns an empty pointer when no dispatch was needed (no requirements, or
/// requirements already satisfied). Throws on failure to set up.
///
/// Called *before* `SystestExecutor::executeSystests` so the rest of the
/// pipeline (logger setup, binder, query engine) runs inside the same
/// CLion-launched process — which is what makes the debug button work for
/// MQTT/external tests the same way it does for regular systests.
std::unique_ptr<ExternalSystestDispatchGuard> maybeDispatchExternalSystest(SystestConfiguration& config);

}
