{
  lib,
  llvmPackages_19,
  cmake,
  ninja,
  pkg-config,
  fetchurl,
  openssl,
  libuuid,
}:

let
  llvm = llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  concatFlags = flags: if flags == [ ] then "" else lib.concatStringsSep " " flags;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
      compilerFlags ? [ ],
      linkerFlags ? [ ],
    }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      libcxxCxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
      ];
      cppLinkerFlags = lib.optionals (useLibcxx || linkerFlags != [ ]) [
        "-DCMAKE_EXE_LINKER_FLAGS=${concatFlags ((lib.optionals useLibcxx [ "-stdlib=libc++" ]) ++ linkerFlags)}"
        "-DCMAKE_SHARED_LINKER_FLAGS=${concatFlags ((lib.optionals useLibcxx [ "-stdlib=libc++" ]) ++ linkerFlags)}"
        "-DCMAKE_MODULE_LINKER_FLAGS=${concatFlags ((lib.optionals useLibcxx [ "-stdlib=libc++" ]) ++ linkerFlags)}"
      ];
      pahoMqttC = stdenv.mkDerivation rec {
        pname = "paho-mqtt-c";
        version = "1.3.14";

        src = fetchurl {
          url = "https://github.com/eclipse/paho.mqtt.c/archive/refs/tags/v${version}.tar.gz";
          hash = "sha512-VXasNTGlxwf5KgLL+51gcQtCrNmfV7zeMRqiJHgCZ6UVLouSprB3r6tHgO4jbV4MKguJhkU0ObzkMjdYs9Q4Ww==";
        };

        patches = [
          ./patches/libuuid-static.patch
          ./patches/libcxx-unsigned-char.patch
        ];

        nativeBuildInputs = [ cmake ninja pkg-config ];
        buildInputs = [ openssl libuuid ] ++ extraBuildInputs;
        cmakeFlags = [
          "-DPAHO_WITH_SSL=TRUE"
          "-DPAHO_HIGH_PERFORMANCE=TRUE"
          "-DPAHO_BUILD_SHARED=FALSE"
          "-DPAHO_BUILD_STATIC=TRUE"
          "-DPAHO_ENABLE_TESTING=FALSE"
        ];
        env = lib.optionalAttrs (compilerFlags != [ ]) {
          NIX_CFLAGS_COMPILE = concatFlags compilerFlags;
        };

        strictDeps = true;
      };
    in
    {
      c = pahoMqttC;
      cpp = stdenv.mkDerivation rec {
        pname = "paho-mqtt-cpp";
        version = "1.5.0";

        src = fetchurl {
          url = "https://github.com/eclipse/paho.mqtt.cpp/archive/refs/tags/v${version}.tar.gz";
          hash = "sha512-0XKu/km2Cm0F6f2G4LOB0vFscARLdZ7aUNFmonyX9FckrczpvHOyaTqw1WHfMJrQexntDUUQ8mSqwDiaMp4PDA==";
        };

        patches = [ ./patches/libcxx-unsigned-char-cpp.patch ];

        nativeBuildInputs = [ cmake ninja pkg-config ];
        buildInputs = [ pahoMqttC openssl ] ++ extraBuildInputs;
        cmakeFlags = [
          "-DPAHO_BUILD_SHARED=FALSE"
          "-DPAHO_BUILD_STATIC=TRUE"
          "-DPAHO_WITH_SSL=TRUE"
        ] ++ libcxxCxxFlags ++ cppLinkerFlags;
        env = lib.optionalAttrs (compilerFlags != [ ]) {
          NIX_CFLAGS_COMPILE = concatFlags compilerFlags;
        };

        strictDeps = true;
      };
    };

  parseWithSanitizerArgs = arg:
    if builtins.isAttrs arg then {
      extraBuildInputs = arg.extraPackages or arg.extraBuildInputs or [ ];
      useLibcxx = arg.useLibcxx or false;
      compilerFlags = arg.compilerFlags or [ ];
      linkerFlags = arg.linkerFlags or [ ];
    } else {
      extraBuildInputs = [ ];
      useLibcxx = false;
      compilerFlags = [ ];
      linkerFlags = [ ];
    };
in
{
  default = build { };
  withSanitizer = arg: build (parseWithSanitizerArgs arg);
}
