{
  lib,
  llvmPackages_19,
  fetchFromGitHub,
  cmake,
  ninja,
  pkg-config,
  boost,
  double-conversion,
  fast-float,
  gflags,
  glog,
  libevent,
  zlib,
  openssl,
  xz,
  lz4,
  zstd,
  libiberty,
  libunwind,
  fmt_11,
}:

let
  llvm = llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  concatFlags = flags: if flags == [ ] then "" else lib.concatStringsSep " " flags;

  follyPatch = ./patches/0001-Folly-Patch.patch;

  baseBuildInputs = [
    boost
    double-conversion
    fast-float
    gflags
    glog
    libevent
    zlib
    openssl
    xz
    lz4
    zstd
    libiberty
    libunwind
  ];

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
      fmtPkg ? null,
      compilerFlags ? [ ],
      linkerFlags ? [ ],
    }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      selectedFmt = if fmtPkg != null then fmtPkg else fmt_11;
      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];
    in
    stdenv.mkDerivation rec {
      pname = "folly";
      version = "unstable-2024-09-04";

      src = fetchFromGitHub {
        owner = "facebook";
        repo = "folly";
        rev = "c47d0c778950043cbbc6af7fde616e9aeaf054ca";
        hash = "sha256-5f7IA/M5oJ/Sg5Z9RMB6PvaHzkXiqn1zZJ+QamZXONs=";
      };

      nativeBuildInputs = [
        cmake
        ninja
        pkg-config
      ];
      buildInputs = lib.unique (baseBuildInputs ++ [ selectedFmt ] ++ extraBuildInputs);
      propagatedBuildInputs = [
        selectedFmt
        boost
      ];

      patches = [ follyPatch ];

      cmakeFlags = [
        "-G"
        "Ninja"
        "-DCMAKE_BUILD_TYPE=Release"
        "-DBUILD_SHARED_LIBS=ON"
        "-DBUILD_TESTS=OFF"
        ("-DFOLLY_USE_LIBCPP=" + (if useLibcxx then "ON" else "OFF"))
        "-DCMAKE_INSTALL_INCLUDEDIR=include"
        "-DCMAKE_INSTALL_LIBDIR=lib"
      ]
      ++ libcxxFlags;

      env = lib.optionalAttrs (compilerFlags != [ ]) {
        NIX_CFLAGS_COMPILE = concatFlags compilerFlags;
      };

      enableParallelBuilding = true;
      strictDeps = true;
      doCheck = false;

      meta = with lib; {
        description = "Facebook open-source C++ library";
        homepage = "https://github.com/facebook/folly";
        license = licenses.asl20;
        platforms = platforms.linux;
      };
    };

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
        fmtPkg = null;
        compilerFlags = [ ];
        linkerFlags = [ ];
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
        fmtPkg = arg.fmtPkg or null;
        compilerFlags = arg.compilerFlags or [ ];
        linkerFlags = arg.linkerFlags or [ ];
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
        fmtPkg = null;
        compilerFlags = [ ];
        linkerFlags = [ ];
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build cfg;
}
