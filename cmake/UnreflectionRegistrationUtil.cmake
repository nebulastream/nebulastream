# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# CMake helpers for unreflection registries: a thin wrapper around the generic runtime
# registry scaffolding (see cmake/RuntimeRegistrationUtil.cmake) that supplies the
# unreflection-specific entry expression. Each registered entry deserializes a Reflected
# payload into the registry's return type via context.unreflect<T>(data).

include(${CMAKE_CURRENT_LIST_DIR}/RuntimeRegistrationUtil.cmake)

# create_unreflection_registry(<name> <target> <type_template> <header_template>)
#   name            : registry name (e.g. LogicalFunction); drives generated symbol names.
#                     The registry class is expected to be ${name}UnreflectionRegistry,
#                     declared in <${name}UnreflectionRegistry.hpp>.
#   target          : parent CMake target that owns the registry. Must already exist.
#   type_template   : format string with "${PLUGIN_NAME}" placeholder; expands to the
#                     C++ type passed to context.unreflect<T>(data).
#                     Examples: "TypedLogicalOperator<${PLUGIN_NAME}LogicalOperator>",
#                               "${PLUGIN_NAME}LogicalFunction", "DataType".
#   header_template : format string with "${PLUGIN_NAME}" placeholder; expands to the
#                     header file basename to #include for each plugin.
#                     Example: "${PLUGIN_NAME}LogicalFunction.hpp"
function(create_unreflection_registry name target type_template header_template)
    create_runtime_registry(${name} ${target}
            REGISTRY_CLASS "${name}UnreflectionRegistry"
            REGISTRY_HEADER "${name}UnreflectionRegistry.hpp"
            HEADER_TEMPLATE "${header_template}"
            GLUE_SUFFIX "unreflection"
            ENTRY_TEMPLATE "[](const Reflected& data, const ReflectionContext& context) {
            return context.unreflect<${type_template}>(data);
        }"
    )
endfunction()

# add_unreflection_plugin(<registry> <plugin_name> [KEY <serialized_name>])
#   plugin_name : substituted into ${PLUGIN_NAME} in the registry's type/header templates.
#                 By convention also the registry key.
#   KEY         : registry lookup key (the string the wire format carries). Defaults to
#                 plugin_name. Use when the registered name differs from the C++ type
#                 basename, e.g. plugin_name "Absolute" / KEY "Abs".
function(add_unreflection_plugin registry plugin_name)
    add_registry_entry(${registry} ${plugin_name} ${ARGN})
endfunction()
