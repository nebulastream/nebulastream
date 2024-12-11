# enables/disables an optional plugin; if enabled, the path to the plugin becomes part of the build
function(activate_optional_plugin plugin_path plugin_option)
    if(${plugin_option})
        message(STATUS "Activating optional plugin: ${plugin_path} (and all of its dependencies).")
        add_subdirectory(${plugin_path})
    else()
        message(STATUS "Skipping optional plugin: ${plugin_path}.")
    endif()
endfunction()

# create a new library for the plugin and link the component that the plugin registry belongs to against it
# adds the name of plugin to the list of plugin names for the plugin registry
# adds the name of the library of the plugin to the list of libraries for the plugin registry
function(add_plugin_as_library plugin_name plugin_registry plugin_registry_component plugin_library)
    set(sources ${ARGN})
    add_library(${plugin_library} STATIC ${sources})
    target_link_libraries(${plugin_library} PRIVATE ${plugin_registry_component})

    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_names" "${plugin_name}")
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_libraries" "${plugin_library}")
endfunction()

# adds the source files of the plugin to the source files of the component that the plugin registry belongs to
# adds the name of plugin to the list of plugin names for the plugin registry
function(add_plugin plugin_name plugin_registry plugin_registry_component)
    set(sources ${ARGN})
    add_source_files(nes-sources
            ${sources}
    )
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_names" "${plugin_name}")
endfunction()

# reads a specific variable, such as 'RETURN_TYPE' from the contents of an '*.inc.in' file
function(read_variable_from_in_file variable_name file_content variable)
    string(REGEX MATCHALL "@(${variable_name}[^@]*)@" matches "${file_content}")
    foreach (match ${matches})
        string(REGEX REPLACE "@([^=]*)=(.*)@" "\\2" value ${match})
        set(${variable} ${value} PARENT_SCOPE)
    endforeach ()
endfunction()

# iterates over all plugins, collect all plugins with given name, inject plugins into registrar
function(generate_plugin_registrar plugin_registry registrar_header_template_path plugin_registry_component)
    # get the names of plugins and all plugin libraries for the plugin registry
    get_property(plugin_registry_plugin_names_final GLOBAL PROPERTY ${plugin_registry}_plugin_names)
    get_property(plugin_registry_plugin_libraries_final GLOBAL PROPERTY ${plugin_registry}_plugin_libraries)

    # first, read the Configuration(RETURN_TYPE, ARGUMENTS) from the '.in' file
    file(READ ${registrar_header_template_path} registrar_header_file_data)
    read_variable_from_in_file("RETURN_TYPE" "${registrar_header_file_data}" return_type)
    read_variable_from_in_file("ARGUMENTS" "${registrar_header_file_data}" arguments)

    # second, remove the configuration and write the modified version of the registrar header template to a temporary file
    # we generate the final '.inc' file from that temporary file
    set(temp_registrar_header_template_file "${CMAKE_CURRENT_BINARY_DIR}/temp_registrar_header_template.inc.in")
    string(REGEX REPLACE "// CONFIGURATION\n.*\n// END CONFIGURATION\n" "" in_file_without_configuration "${registrar_header_file_data}")
    file(WRITE ${temp_registrar_header_template_file} "${in_file_without_configuration}")

    # generate the list of declarations of the register functions that the plugins implement to register themselves
    # generate the list of concrete register calls that are called in the 'registerAll' function call of the Registrar to populate the registry
    set(REGISTER_FUNCTION_DECLARATIONS "")
    set(REGISTER_ALL_FUNCTION_CALLS "")
    foreach (reg_func IN LISTS plugin_registry_plugin_names_final)
        list(APPEND REGISTER_FUNCTION_DECLARATIONS "${return_type} Register${reg_func}${plugin_registry}(${arguments})")
        list(APPEND REGISTER_ALL_FUNCTION_CALLS "registry.registerPlugin(\"${reg_func}\", Register${reg_func}${plugin_registry})")
    endforeach ()

    # link all plugin libraries against the component that the plugin registry belongs to, this makes the implementation
    # details accessible to the library of the component that the plugin registry belongs to
    foreach (plugin_library IN LISTS plugin_registry_plugin_libraries_final)
        target_link_libraries(${plugin_registry_component} PUBLIC ${plugin_library})
    endforeach ()

    # add ';'s to the end of the generated function [declarations|calls], and add further formatting (new line and tab)
    string(REPLACE ";" ";\n" REGISTER_FUNCTION_DECLARATIONS "${REGISTER_FUNCTION_DECLARATIONS};")
    string(REPLACE ";" ";\n\t" REGISTER_ALL_FUNCTION_CALLS "${REGISTER_ALL_FUNCTION_CALLS};")

    # remove the '.in' from the end of the file and write the result to the parent directory of the the template
    # we assume that the Registrar templates are in a subdirectory, e.g., 'PATH/TO/REGISTRAR/RegistrarTemplates'
    string(REGEX REPLACE "/[^/]+/([^/]+)\.in$" "/\\1" registrar_header_generated_path ${registrar_header_template_path})
    configure_file(
            ${temp_registrar_header_template_file}
            ${registrar_header_generated_path}
            @ONLY
    )
    file(REMOVE ${temp_registrar_header_template_file})
endfunction()
