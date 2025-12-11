{ lib
, llvmPackages_19
, cmake
, ninja
, pkg-config
, fetchFromGitHub
, libdwarf
, zstd
, writeTextFile
}:

let
  clangStdenv = llvmPackages_19.stdenv;

  libdwarfModule = writeTextFile {
    name = "libdwarf-cmake";
    destination = "/share/cmake/Modules/Findlibdwarf.cmake";
    text = ''
      set(libdwarf_INCLUDE_DIR "${libdwarf.dev}/include/libdwarf-2")
      find_library(libdwarf_LIBRARY
        NAMES dwarf libdwarf
        PATHS
          "${(libdwarf.lib or libdwarf)}/lib"
          "${libdwarf}/lib"
      )
      include(FindPackageHandleStandardArgs)
      find_package_handle_standard_args(libdwarf DEFAULT_MSG libdwarf_INCLUDE_DIR libdwarf_LIBRARY)
      if(libdwarf_FOUND)
        add_library(libdwarf::libdwarf UNKNOWN IMPORTED)
        set_target_properties(libdwarf::libdwarf PROPERTIES
          IMPORTED_LOCATION ''${libdwarf_LIBRARY}
          INTERFACE_INCLUDE_DIRECTORIES ''${libdwarf_INCLUDE_DIR})
        set(libdwarf_INCLUDE_DIRS ''${libdwarf_INCLUDE_DIR})
        set(LIBDWARF_INCLUDE_DIRS ''${libdwarf_INCLUDE_DIR})
        set(libdwarf_LIBRARIES libdwarf::libdwarf)
        set(LIBDWARF_LIBRARIES libdwarf::libdwarf)
      endif()
    '';
  };

in clangStdenv.mkDerivation rec {
  pname = "cpptrace";
  version = "1.0.4";

  src = fetchFromGitHub {
    owner = "jeremy-rifkin";
    repo = "cpptrace";
    rev = "v${version}";
    hash = "sha512-jK2VnWtk7ovSFhgDkwqAlQ5REgVzyqU2nddySnuZZSIVLP5nW5EnOpfLGTpg65x+9ILaCm1UacouvIyJfzyYHQ==";
  };

  nativeBuildInputs = [
    cmake
    ninja
    pkg-config
  ];
  buildInputs = [
    libdwarf.dev
    zstd.dev
  ];

  cmakeFlags = [
    "-G"
    "Ninja"
    "-DCMAKE_BUILD_TYPE=Release"
    "-DCPPTRACE_USE_EXTERNAL_LIBDWARF=ON"
    "-DCPPTRACE_USE_EXTERNAL_ZSTD=ON"
    "-DCPPTRACE_BUILD_TESTS=OFF"
    "-DCPPTRACE_BUILD_EXAMPLES=OFF"
    "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
  ];

  enableParallelBuilding = true;
  strictDeps = true;

  meta = with lib; {
    description = "C++ stack trace library";
    homepage = "https://github.com/jeremy-rifkin/cpptrace";
    license = licenses.mit;
    platforms = platforms.linux;
  };
}
