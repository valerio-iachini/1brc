{
  inputs = {
    systems.url = "github:nix-systems/default";
    nixpkgs.url = "github:NixOS/nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { systems, nixpkgs, rust-overlay, ... }: let
    overlays = [ rust-overlay.overlays.default ];

    eachSystem = f:
      nixpkgs.lib.genAttrs (import systems) (system:
        f (import nixpkgs { inherit system overlays; })
      );

  in {
    devShells = eachSystem (pkgs: {
      default = pkgs.mkShell {
        buildInputs = [
          pkgs.rust-bin.nightly.latest.default
          pkgs.rust-analyzer
          pkgs.perf
        ];
      };
    });
  };
}
