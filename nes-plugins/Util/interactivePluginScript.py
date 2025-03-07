#!/usr/bin/env python3
import argparse
import sys
import os
import re

def replace_placeholders(template, namespace, name_of_registry):
    """Replace all placeholders in the template with the provided values."""
    result = template.replace('@THE_NAMESPACE@', namespace)
    result = result.replace('@THE_NAME_OF_THE_REGISTRY@', name_of_registry)
    result = result.replace('@THE_NAME_OF_THE_REGISTRY_CAPS@', name_of_registry.upper())
    return result

def print_registries(nes_root_dir: str, path_to_parent_dir: str, registries: list):
    print(f"📁 {nes_root_dir}/")
    single_indent = "    "
    print(f"{single_indent}└── 📁 nes-plugins/")
    for idx, subdir in enumerate(registries):
        prefix = "└── " if idx == len(registries) - 1 else "├── "

        if os.path.isdir(os.path.join(path_to_parent_dir,subdir)):
            print(f"{single_indent}{single_indent}{prefix}📁 {subdir}/ ← REGISTRY")

def print_directory_tree(path, nes_root_dir, highlight_dir=None, new_dir=None) -> str:
    """Print a directory tree structure for a path, optionally highlighting specific directories."""
    # Split the path into components
    single_indent = "    "
    path_components = os.path.normpath(path).split(os.sep)

    # Process each component of the path
    indent = ""
    total_path = os.path.sep
    found_root_dir = False
    print(f"📁 {nes_root_dir}/")
    for i in range(1, len(path_components)):
        component = path_components[i]
        total_path = os.path.join(total_path, component)
        if not found_root_dir:
            found_root_dir = (component == nes_root_dir)
            continue

        # Increase indent for each level
        indent += single_indent

        print(f"{indent}└── 📁 {component}/")

        # If we're at the final component, print its contents
        if i == len(path_components) - 1:
            try:
                subdirs = [d for d in os.listdir(total_path)]
                # subdirs = [d for d in os.listdir(current_path) if os.path.isdir(os.path.join(current_path, d))]
                subdirs.sort()

                sub_indent = indent + single_indent
                for idx, subdir in enumerate(subdirs):
                    prefix = "└── " if idx == len(subdirs) - 1 else "├── "

                    if highlight_dir and subdir.lower() == highlight_dir.lower():
                        print(f"{sub_indent}{prefix}📁 {subdir}/ ← EXISTS")
                    elif new_dir and subdir.lower() == new_dir.lower():
                        if os.path.isdir(subdir):
                            print(f"{sub_indent}{prefix}📁 {subdir}/ ← NEW")
                        else:
                            print(f"{sub_indent}{prefix}📁 {subdir} ← NEW")
                    else:
                        if os.path.isdir(os.path.join(total_path, subdir)):
                            print(f"{sub_indent}{prefix}📁 {subdir}/")
                        else:
                            print(f"{sub_indent}{prefix}📁 {subdir}")

            except PermissionError:
                print(f"{indent + single_indent}├── (Permission denied)")
            except Exception as e:
                print(f"{indent + single_indent}├── (Error: {e})")
    return indent + single_indent

def get_all_registries(parent_dir, script_dir):
    """Get all directories in the parent directory except the script directory."""
    script_dir_name = os.path.basename(script_dir)
    try:
        return [d for d in os.listdir(parent_dir)
                if os.path.isdir(os.path.join(parent_dir, d)) and d != script_dir_name]
    except Exception as e:
        print(f"Error listing directories: {e}")
        return []

def capitalize_first_letter(string):
    """Capitalize the first letter of a string and make the rest lowercase."""
    if not string:
        return string
    return string[0].upper() + string[1:]

def interactive_mode(nes_root_dir: str, nes_root_path: str):
    """Run the script in interactive mode, asking the user what they want to do."""
    # Get the script directory and its parent
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)

    print("Welcome to the Registry Generator!")
    print("=================================")
    print("What would you like to do?")
    print("1. Create a new plugin registry")
    print("2. Create a new plugin for an existing plugin registry (not implemented yet)")
    print("3. List all existing registries")

    choice = input("\nEnter your choice (1-3): ")
    if choice == "1":
        # Create a new plugin registry
        print("\n=== Create a new plugin registry ===")

        # Ask for registry name
        registry_name = input("Enter the name of the registry: ")

        # Check if registry already exists
        registries = get_all_registries(parent_dir, script_dir)
        registry_exists = any(r.lower() == registry_name.lower() for r in registries)

        if registry_exists:
            print(f"\nRegistry '{registry_name}' already exists!")
            print("\nCurrent directory structure:")
            print_directory_tree(parent_dir, registry_name)
        else:
            # Format registry name (capitalized first letter)
            formatted_registry_name = capitalize_first_letter(registry_name)

            # Ask for namespace
            namespace = input("\nEnter the namespace for the registry: ")

            # Ask for output directory
            output_dir = os.path.join(nes_root_path, input(f"\nEnter the path where registry files should be written: {nes_root_path}/"))
            if not os.path.isdir(output_dir):
                create_dir = input(f"Directory '{output_dir}' does not exist. Create it? (y/n): ")
                if create_dir.lower() == 'y':
                    try:
                        os.makedirs(output_dir)
                        print(f"Created directory: {output_dir}")
                    except OSError as e:
                        print(f"Error creating directory: {e}")
                        return
                else:
                    print("Operation cancelled.")
                    return

            # Create the registry directory in parent dir
            registry_dir = os.path.join(parent_dir, formatted_registry_name)

            # Show the directory structure that would be created
            print("\nDirectory structure that will be created:")
            first_indent = print_directory_tree(parent_dir, nes_root_dir)
            print(f"{first_indent}└── 📁 {formatted_registry_name}/ ← NEW")

            second_indent = print_directory_tree(os.path.join(output_dir, "include"), nes_root_dir)
            print(f"{second_indent}└── 📁 {formatted_registry_name}Registry.hpp ← NEW")

            second_indent = print_directory_tree(os.path.join(output_dir, "generated"), nes_root_dir)
            print(f"{second_indent}└── 📁 {formatted_registry_name}GeneratedRegistrar.inc.in ← NEW")

            # Confirm creation
            confirm = input("\nCreate registry files? (y/n): ")
            if confirm.lower() != 'y':
                print("Operation cancelled.")
                return

            # Create registry directory
            try:
                os.makedirs(registry_dir, exist_ok=True)
                print(f"Created registry directory: {registry_dir}")
            except OSError as e:
                print(f"Error creating registry directory: {e}")
                return

            # Generate registry files
            generate_registry_files(output_dir, namespace, formatted_registry_name)

    elif choice == "2":
        # Create a new plugin for an existing plugin registry
        print("\nTodo: Create a new plugin for an existing plugin registry")

    elif choice == "3":
        # List all existing registries
        print("\n=== Existing Registries ===")
        registries = get_all_registries(parent_dir, script_dir)

        if registries:
            print_registries(nes_root_dir, parent_dir, registries)
        else:
            print("No registries found.")
    else:
        print("Invalid choice. Please select 1, 2, or 3.")

def generate_registry_files(output_dir, namespace, name_of_registry):
    """Generate registry files in the specified output directory."""
    # Determine if we need to create the generated directory
    registry_dir = os.path.join(output_dir, 'include')
    generated_dir = os.path.join(output_dir, 'generated')

    # Registry template
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

    # Registrar template
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

    # Replace placeholders
    registry_result = replace_placeholders(registryTemplate, namespace, name_of_registry)
    registrar_result = replace_placeholders(registrarTemplate, namespace, name_of_registry)

    # Create generated directory if needed
    if not os.path.exists(generated_dir):
        try:
            os.makedirs(generated_dir)
            print(f"Created generated directory: {generated_dir}")
        except OSError as e:
            print(f"Error creating generated directory: {e}")
            return

    # Write registry file
    try:
        registry_file_path = os.path.join(registry_dir, f"{name_of_registry}Registry.hpp")
        with open(registry_file_path, 'w') as f:
            f.write(registry_result)
        print(f"Output written to {registry_file_path}")
    except IOError as e:
        print(f"Error writing to file {registry_file_path}: {e}")
        return

    # Write registrar file
    try:
        registrar_file_path = os.path.join(generated_dir, f"{name_of_registry}GeneratedRegistrar.inc.in")
        with open(registrar_file_path, 'w') as f:
            f.write(registrar_result)
        print(f"Output written to {registrar_file_path}")
    except IOError as e:
        print(f"Error writing to file {registrar_file_path}: {e}")
        return

def find_project_root():
    # Get the absolute path of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Check if the current path or any parent contains 'nes-plugins'
    current_path = script_dir

    # Get the directory name of the current path
    parent_dir = os.path.dirname(current_path)
    dir_name = os.path.basename(parent_dir)

    # If we found 'nes-plugins', return its parent directory
    if dir_name.lower() == 'nes-plugins':
        return os.path.dirname(parent_dir)

    return None

def main():
    nes_root_path = find_project_root()
    if nes_root_path is None:
        print("Error: This script must be executed within the 'nes-plugins/Util' directory structure.")
        sys.exit(1)

    if len(sys.argv) == 1:
        interactive_mode(os.path.basename(os.path.normpath(nes_root_path)), nes_root_path)
        return
    else:
        print("The script does not expect arguments.")


if __name__ == "__main__":
    main()