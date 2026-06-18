{
  lib,
  llvmPackages,
  cmake,
  ninja,
  pkg-config,
  python3,
  fetchFromGitHub,
}:

let
  clangStdenv = llvmPackages.stdenv;

  stablehloSrc = fetchFromGitHub {
    owner = "iree-org";
    repo = "stablehlo";
    rev = "46af9d3590bc712d1c9e2f8ef408d2ecbb6108a3";
    hash = "sha256-yeDKEteSDyNOCRvxsMRhp8RbgDMr6qVm+voMNs9Vs8M=";
  };

  flatccSrc = fetchFromGitHub {
    owner = "dvidelabs";
    repo = "flatcc";
    rev = "9362cd00f0007d8cbee7bff86e90fb4b6b227ff3";
    hash = "sha256-umZ9TvNYDZtF/mNwQUGuhAGve0kPw7uXkaaQX0EzkBY=";
  };

  printfSrc = fetchFromGitHub {
    owner = "eyalroz";
    repo = "printf";
    rev = "f1b728cbd5c6e10dc1f140f1574edfd1ccdcbedb";
    hash = "sha256-HHp6uKEJv3HWEGgIBjeMsCXUSIPYTLwofjcDTswgSuA=";
  };

in
clangStdenv.mkDerivation rec {
  pname = "ireeruntime";
  version = "3.11.0";

  src = fetchFromGitHub {
    owner = "iree-org";
    repo = "iree";
    rev = "4fa6868eea932128282aa3df38a771a4032ba42a";
    hash = "sha256-2NHWLhhCoCHJGWN+ZPZm44pOeksr3fTpLH7X0hkThWw=";
  };

  # Note: 0002-fixup-install-destination.patch is NOT used here — it prepends
  # CMAKE_INSTALL_PREFIX to install destinations, which causes doubled paths
  # under Nix (Nix already sets CMAKE_INSTALL_PREFIX correctly).
  patches = [
    ./patches/0001-runtime-only-build.patch
    ./patches/0003-do-not-enable-null-driver-in-debug.patch
    ./patches/0004-fix-missing-check-c-source-compiles-include.patch
    ./patches/0005-include-printf-in-install.patch
  ];

  postPatch = ''
    rm -rf third_party/stablehlo third_party/flatcc third_party/printf
    ln -s ${stablehloSrc} third_party/stablehlo
    ln -s ${flatccSrc} third_party/flatcc
    ln -s ${printfSrc} third_party/printf
  '';

  nativeBuildInputs = [
    cmake
    ninja
    pkg-config
    python3
  ];

  cmakeFlags = [
    "-DIREE_BUILD_TESTS=OFF"
    "-DIREE_BUILD_COMPILER=OFF"
    "-DIREE_BUILD_SAMPLES=OFF"
    "-DIREE_BUILD_ALL_CHECK_TEST_MODULES=OFF"
    "-DIREE_BUILD_BUNDLED_LLVM=OFF"
    "-DIREE_BUILD_BINDINGS_TFLITE=OFF"
    "-DIREE_BUILD_BINDINGS_TFLITE_JAVA=OFF"
    "-DIREE_ENABLE_THREADING=OFF"
    "-DIREE_HAL_DRIVER_DEFAULTS=OFF"
    "-DIREE_HAL_DRIVER_LOCAL_SYNC=ON"
    "-DIREE_TARGET_BACKEND_DEFAULTS=OFF"
    "-DIREE_ERROR_ON_MISSING_SUBMODULES=OFF"
    "-DIREE_ENABLE_CPUINFO=OFF"
    "-DIREE_ENABLE_LIBBACKTRACE=OFF"
    "-DIREE_INPUT_TORCH=OFF"
  ];

  buildFlags = [ "all" ];

  installTargets = [ "iree-install-dist" ];

  postInstall = ''
    # Copy runtime source headers (needed by NES at compile time)
    runtimeHeaderDir="$src/runtime/src"
    find "$runtimeHeaderDir" -name '*.h' | while IFS= read -r header; do
      rel="''${header#$runtimeHeaderDir/}"
      dest="$out/include/$rel"
      mkdir -p "$(dirname "$dest")"
      cp "$header" "$dest"
    done

    # Consolidate all cmake config/targets files into lib/cmake/IREERuntime
    mkdir -p $out/lib/cmake/IREERuntime

    # Find and move all IREE cmake files to the standard location
    find $out -name '*.cmake' -path '*/IREE*' | while IFS= read -r f; do
      base="$(basename "$f")"
      dest="$out/lib/cmake/IREERuntime/$base"
      if [ "$f" != "$dest" ]; then
        mv "$f" "$dest"
      fi
    done

    # Also check for cmake files at the package root (iree_generate_export_targets
    # installs to CMAKE_INSTALL_PREFIX directly without the fixup patch)
    for f in $out/IREETargets-Runtime*.cmake; do
      [ -f "$f" ] && mv "$f" $out/lib/cmake/IREERuntime/
    done

    # Clean up empty cmake dirs
    find $out/lib/cmake -type d -empty -delete 2>/dev/null || true
  '';

  meta = with lib; {
    description = "IREE runtime for ML inference";
    homepage = "https://iree.dev";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
}
