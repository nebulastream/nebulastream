# Detect if the install is run by CPack.
if (${CMAKE_INSTALL_PREFIX} MATCHES "/_CPack_Packages/.*/(TGZ|ZIP)/")
    message("CMAKE_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX}")
    cmake_path(GET CMAKE_INSTALL_PREFIX PARENT_PATH CMAKE_INSTALL_PREFIX_PARENT)
    cmake_path(GET CMAKE_INSTALL_PREFIX_PARENT PARENT_PATH CMAKE_INSTALL_PREFIX_PARENT)
    message("CMAKE_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX_PARENT}")
    # Flatten the directory structure such that everything except the header files is placed in root.
    file(COPY ${CMAKE_INSTALL_PREFIX}/bin/ DESTINATION ${CMAKE_INSTALL_PREFIX_PARENT}/bin/)
    file(COPY ${CMAKE_INSTALL_PREFIX}/lib/ DESTINATION ${CMAKE_INSTALL_PREFIX_PARENT}/lib/)
    file(COPY ${CMAKE_INSTALL_PREFIX}/include/ DESTINATION ${CMAKE_INSTALL_PREFIX_PARENT}/include/)

    execute_process( COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_INSTALL_PREFIX}/bin)
    execute_process( COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_INSTALL_PREFIX}/lib)
    execute_process( COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_INSTALL_PREFIX}/include)
endif()