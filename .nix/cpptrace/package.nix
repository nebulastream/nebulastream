{
  lib,
  llvmPackages_19,
  cmake,
  ninja,
  pkg-config,
  fetchFromGitHub,
  libdwarf,
  zstd,
  writeTextFile,
}:

let
  llvmPackages = llvmPackages_19;
  clangStdenv = llvmPackages.stdenv;
  libcxxStdenv = llvmPackages.libcxxStdenv;

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

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
    }:
    let
      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];
    in
    (if useLibcxx then libcxxStdenv else clangStdenv).mkDerivation rec {
      pname = "cpptrace";
      version = "0.8.3";

      src = fetchFromGitHub {
        owner = "jeremy-rifkin";
        repo = "cpptrace";
        rev = "v${version}";
        hash = "sha512-T+fmn1DvgAhUBjanRJBcXc3USAJe4Qs2v5UpiLj+HErLtRKoOCr9V/Pa5Nfpfla9v5H/q/2REKpBJ3r4exSSoQ==";
      };

      nativeBuildInputs = [
        cmake
        ninja
        pkg-config
      ];
      buildInputs = [
        libdwarf.dev
        zstd.dev
      ]
      ++ extraBuildInputs;

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DCMAKE_BUILD_TYPE=Release"
        "-DBUILD_SHARED_LIBS=ON"
        "-DCPPTRACE_USE_EXTERNAL_LIBDWARF=ON"
        "-DCPPTRACE_USE_EXTERNAL_ZSTD=ON"
        "-DCPPTRACE_BUILD_TESTS=OFF"
        "-DCPPTRACE_BUILD_EXAMPLES=OFF"
        "-DCPPTRACE_BUILD_SHARED=ON"
        "-DCMAKE_MODULE_PATH=${libdwarfModule}/share/cmake/Modules"
      ]
      ++ libcxxFlags;

      enableParallelBuilding = true;
      strictDeps = true;

      meta = with lib; {
        description = "C++ stack trace library";
        homepage = "https://github.com/jeremy-rifkin/cpptrace";
        license = licenses.mit;
        platforms = platforms.linux;
      };
    };

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
      }
    else if builtins.isAttrs arg then
      {
        extraBuildInputs =
          if arg ? extraBuildInputs then
            arg.extraBuildInputs
          else if arg ? extraPackages then
            arg.extraPackages
          else
            [ ];
        useLibcxx = arg.useLibcxx or false;
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build {
      inherit (cfg) extraBuildInputs useLibcxx;
    };
}
