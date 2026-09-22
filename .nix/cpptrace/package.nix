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

  # Find module for the nixpkgs libdwarf this derivation builds cpptrace against.
  #
  # This mirrors the canonical shim at `cmake/Findlibdwarf.cmake`: that file cannot be reused
  # directly here since this derivation resolves libdwarf via a nix store path rather than
  # pkg-config/find_library discovery, but the target name it exports must stay in sync with the
  # variables it sets (libdwarf_LIBRARIES / LIBDWARF_LIBRARIES): the canonical target name is
  # `libdwarf::dwarf` (what cpptrace's own vcpkg build path links against, and what the real
  # upstream libdwarf CMake config exports); `libdwarf::libdwarf` is kept as an alias for anything
  # still written against the older name.
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
        add_library(libdwarf::dwarf UNKNOWN IMPORTED)
        set_target_properties(libdwarf::dwarf PROPERTIES
          IMPORTED_LOCATION ''${libdwarf_LIBRARY}
          INTERFACE_INCLUDE_DIRECTORIES ''${libdwarf_INCLUDE_DIR})
        add_library(libdwarf::libdwarf ALIAS libdwarf::dwarf)
        set(libdwarf_INCLUDE_DIRS ''${libdwarf_INCLUDE_DIR})
        set(LIBDWARF_INCLUDE_DIRS ''${libdwarf_INCLUDE_DIR})
        set(libdwarf_LIBRARIES libdwarf::dwarf)
        set(LIBDWARF_LIBRARIES libdwarf::dwarf)
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
      version = "1.0.4";

  src = fetchFromGitHub {
    owner = "jeremy-rifkin";
    repo = "cpptrace";
    rev = "v${version}";
    hash = "sha512-jK2VnWtk7ovSFhgDkwqAlQ5REgVzyqU2nddySnuZZSIVLP5nW5EnOpfLGTpg65x+9ILaCm1UacouvIyJfzyYHQ==";
  };

   patches = [
      ./patches/0001-bump-cxx-std.patch
   ];

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
