#!/usr/bin/env python3
import argparse
import sys
import os

def replace_placeholders(template, namespace, name_of_registry):
    """Replace all placeholders in the template with the provided values."""
    result = template.replace('@THE_NAMESPACE@', namespace)
    result = result.replace('@THE_NAME_OF_THE_REGISTRY@', name_of_registry)
    result = result.replace('@THE_NAME_OF_THE_REGISTRY_CAPS@', name_of_registry.upper())
    return result

def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description='Generate registry code from template.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Print both files to console
  %(prog)s The::Namespace TheRegistry
  
  # Print only Registry file to console
  %(prog)s The::Namespace TheRegistry --mode RegistryOnly
  
  # Print only Registrar file to console
  %(prog)s The::Namespace TheRegistry --mode RegistrarOnly
  
  # Write both files to output directory
  %(prog)s The::Namespace TheRegistry -o ./output/directory
  
  # Write only Registry file to output directory
  %(prog)s The::Namespace TheRegistry -o ./output/directory --mode RegistryOnly
  
  # Write only Registrar file to output directory
  %(prog)s The::Namespace TheRegistry -o ./output/directory --mode RegistrarOnly
        '''
    )
    parser.add_argument('namespace', help='The namespace to use in the template')
    parser.add_argument('nameOfRegistry', help='The name of the registry to use in the template')
    parser.add_argument('-o', '--output', help='Output directory (if not specified, print to console)')
    parser.add_argument('--mode', choices=['RegistryOnly', 'RegistrarOnly'],
                        help='Specify which file to generate. If not specified, both files are generated')

    args = parser.parse_args()

# First template content (Registry.hpp)
    registryTemplate = '''/*
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

// TODO: includes
#include <Util/Registry.hpp>

namespace @THE_NAMESPACE@
{

using @THE_NAME_OF_THE_REGISTRY@RegistryReturnType = /* TODO: return type of registry */;
struct @THE_NAME_OF_THE_REGISTRY@RegistryArguments
{
    // TODO: arguments for registry
};

class @THE_NAME_OF_THE_REGISTRY@Registry : public BaseRegistry<@THE_NAME_OF_THE_REGISTRY@Registry, std::string, @THE_NAME_OF_THE_REGISTRY@RegistryReturnType, @THE_NAME_OF_THE_REGISTRY@RegistryArguments>
{
};

}

#define INCLUDED_FROM_@THE_NAME_OF_THE_REGISTRY_CAPS@_REGISTRY
#include <@THE_NAME_OF_THE_REGISTRY@GeneratedRegistrar.inc>
#undef INCLUDED_FROM_@THE_NAME_OF_THE_REGISTRY_CAPS@_REGISTRY'''


# Second template content (GeneratedRegistrar.inc.in)
    registrarTemplate = '''/*
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

/// NOLINT(clang-diagnostic-error)
#ifndef INCLUDED_FROM_@THE_NAME_OF_THE_REGISTRY_CAPS@_REGISTRY
#    error "This file should not be included directly! Include instead include @THE_NAME_OF_THE_REGISTRY@Registry.hpp"
#endif

#include <string>

/// @Note: DO NOT MODIFY THIS FILE unless you are very sure that you need to.
/// We usually generate this file using the script in 'nes-plugins/Util/boostrapRegistry.py'
/// During our CMake build process, we use CMake codegen to generate an '.inc' file from this 'inc.in' file.
/// In that process, we generate and insert the function declarations for the register functions of the plugins
/// of this registry into the REGISTER_FUNCTION_DECLARATIONS placeholder.
/// Similarly, we insert the function calls of the register functions of the plugins of this registry into the
/// REGISTER_ALL_FUNCTION_CALLS placeholder. Inserting the function calls into the 'registerAll()' function means that
/// when we request an instance of this registry for the first time, we call 'registerAll()',
/// which then calls all register functions of all plugins of this registry, populating the registry.
namespace @THE_NAMESPACE@::@THE_NAME_OF_THE_REGISTRY@GeneratedRegistrar
{

/// declaration of register functions for 'Sources'
@REGISTER_FUNCTION_DECLARATIONS@
}

namespace NES
{
template <>
inline void
Registrar<@THE_NAMESPACE@::@THE_NAME_OF_THE_REGISTRY@Registry, std::string, @THE_NAMESPACE@::@THE_NAME_OF_THE_REGISTRY@RegistryReturnType, @THE_NAMESPACE@::@THE_NAME_OF_THE_REGISTRY@RegistryArguments>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{

    using namespace @THE_NAMESPACE@::@THE_NAME_OF_THE_REGISTRY@GeneratedRegistrar;
    /// the @THE_NAME_OF_THE_REGISTRY@Registry calls registerAll and thereby all the below functions that register @THE_NAME_OF_THE_REGISTRY@s in the SourceRegistry
    @REGISTER_ALL_FUNCTION_CALLS@
}
}'''

    # Determine which files to generate
    generate_registry = args.mode != 'RegistrarOnly'
    generate_registrar = args.mode != 'RegistryOnly'

    # Replace placeholders in the generated that will be used
    registry_result = replace_placeholders(registryTemplate, args.namespace, args.nameOfRegistry) if generate_registry else None
    registrar_result = replace_placeholders(registrarTemplate, args.namespace, args.nameOfRegistry) if generate_registrar else None

    # Handle output based on whether -o was supplied
    if args.output:
        # Check if output is a directory
        if not os.path.isdir(args.output):
            print(f"Error: '{args.output}' is not a valid directory", file=sys.stderr)
            sys.exit(1)

        # Generate Registry file if needed
        if generate_registry:
            try:
                registry_file_path = os.path.join(args.output, f"{args.nameOfRegistry}Registry.hpp")
                with open(registry_file_path, 'w') as f:
                    f.write(registry_result)
                print(f"Output written to {registry_file_path}")
            except IOError as e:
                print(f"Error writing to file {registry_file_path}: {e}", file=sys.stderr)
                sys.exit(1)

        # Generate Registrar file if needed
        if generate_registrar:
            # Create generated directory if it doesn't exist
            generated_dir = os.path.join(args.output, '../generated')
            if not os.path.exists(generated_dir):
                try:
                    os.makedirs(generated_dir)
                    print(f"Created generated directory: {generated_dir}")
                except OSError as e:
                    print(f"Error creating generated directory: {e}", file=sys.stderr)
                    sys.exit(1)

            try:
                registrar_file_path = os.path.join(generated_dir, f"{args.nameOfRegistry}GeneratedRegistrar.inc.in")
                with open(registrar_file_path, 'w') as f:
                    f.write(registrar_result)
                print(f"Output written to {registrar_file_path}")
            except IOError as e:
                print(f"Error writing to file {registrar_file_path}: {e}", file=sys.stderr)
                sys.exit(1)
    else:
        # Print to console based on which files are selected
        if generate_registry:
            print("=== File 1: {0}Registry.hpp ===".format(args.nameOfRegistry))
            print(registry_result)

        if generate_registry and generate_registrar:
            print("\n\n")  # Add separation between files

        if generate_registrar:
            print("=== File 2: {0}GeneratedRegistrar.inc.in ===".format(args.nameOfRegistry))
            print(registrar_result)

if __name__ == "__main__":
    main()
