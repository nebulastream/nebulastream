{
  fetchFromGitHub,
  llvmPackages_19,
  lib,
  cmake,
  nix-update-script,
}:
llvmPackages_19.stdenv.mkDerivation (finalAttrs: {
  pname = "nameof";
  version = "0.10.4";

  src = fetchFromGitHub {
    owner = "Neargye";
    repo = "nameof";
    tag = "v${finalAttrs.version}";
    hash = "sha256-g3VwgCloqFnghG7OB7aK3uB/1RNKe8h6Nu7pbIGqrEQ=";
  };

  nativeBuildInputs = [ cmake ];

  cmakeFlags = [
    # the cmake package does not handle absolute CMAKE_INSTALL_INCLUDEDIR correctly
    # (setting it to an absolute path causes include files to go to $out/$out/include,
    #  because the absolute path is interpreted with root at $out).
    (lib.cmakeFeature "CMAKE_INSTALL_INCLUDEDIR" "include")
    (lib.cmakeFeature "CMAKE_INSTALL_LIBDIR" "lib")
  ];

  meta = {
    description = "Nameof operator for modern C++, simply obtain the name of a variable, type, function, macro, and enum ";
    homepage = "https://github.com/Neargye/nameof";
    changelog = "https://github.com/Neargye/nameof/releases/tag/v${finalAttrs.version}";
    license = lib.licenses.mit;
  };
})
