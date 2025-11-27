{
  fetchFromGitHub,
  llvmPackages_19,
  lib,
  cmake,
  nix-update-script,
}:
llvmPackages_19.stdenv.mkDerivation (finalAttrs: {
  pname = "scope_guard";
  version = "0.9.1";

  src = fetchFromGitHub {
    owner = "Neargye";
    repo = "scope_guard";
    tag = "v${finalAttrs.version}";
    hash = "sha256-Bs8dr5t9LQ6FyM6btpDBAEvyiKoCF2H99RRFAQfx7/I=";
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
    description = "Scope Guard & Defer C++";
    homepage = "https://github.com/Neargye/scope_guard";
    changelog = "https://github.com/Neargye/scope_guard/releases/tag/v${finalAttrs.version}";
    license = lib.licenses.mit;
  };
})
