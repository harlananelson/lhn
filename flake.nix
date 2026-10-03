{
  description = "lhn development environment (PySpark healthcare data extraction)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      python = pkgs.python312;
      jdk = pkgs.jdk17;
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        name = "lhn";

        packages = [
          python
          pkgs.uv
          jdk # PySpark needs a JVM
          pkgs.git
          pkgs.jq
          pkgs.tmux
        ];

        JAVA_HOME = "${jdk}";

        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
          pkgs.stdenv.cc.cc.lib
          pkgs.zlib
        ];

        shellHook = ''
          # nix-name: give this shell a name when getpwuid has none for the uid.
          # Pure nix shells on some hosts otherwise prompt "I have no name!".
          # No LD_PRELOAD. The nixpkgs nss_wrapper is linked to a newer glibc than
          # the host, and preloading it breaks host binaries started from the shell.
          if ! id -un >/dev/null 2>&1; then
          _nn_dir="''${TMPDIR:-/tmp}/nix-name-$$"
          mkdir -p "$_nn_dir"
          _nn_name=$(logname 2>/dev/null || printf '%s' "''${USER:-nixuser}")
          printf '%s\n' '#!/bin/sh' "printf '%s\\n' '$_nn_name'" > "$_nn_dir/whoami"
          chmod +x "$_nn_dir/whoami"
          export PATH="$_nn_dir:''${PATH}"
          export USER="$_nn_name"
          export LOGNAME="$_nn_name"
          export PS1="$_nn_name@\h:\w\\$ "
          unset _nn_dir _nn_name
          fi
          export USER=''${USER:-$(whoami)}

          # Create venv on first entry: sibling spark_config_mapper + lhn editable, plus dev extras
          if [ ! -d .venv ]; then
            echo "Creating Python venv and installing lhn (editable)..."
            uv venv .venv
            uv pip install --python .venv/bin/python \
              -e ../spark_config_mapper \
              -e ".[dev,plotting]"
          fi
          source .venv/bin/activate

          echo "lhn dev shell ready. Python: $(python --version), Java: $(java -version 2>&1 | head -1)"
          echo "Note: HDL runs Spark 2.4.4; local pyspark is newer. Test against HDL for API compatibility."
        '';
      };
    };
}
