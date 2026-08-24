#!/usr/bin/env bash
# Verify the container runtime is live before `make up` tries to drive it.
#
# This project uses **podman, not docker**. That is not a stylistic preference:
# the `docker-compose` on this host is a genuine Docker Compose binary that talks
# to /var/run/docker.sock, and there is no Docker daemon running — it fails with
# "permission denied while trying to connect to the docker API". podman-compose
# drives rootless podman (4.9.3) directly and works.
set -euo pipefail

if ! command -v podman >/dev/null 2>&1; then
  echo "ERROR: podman is not on PATH. Install it with your package manager." >&2
  exit 1
fi

if ! command -v podman-compose >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: podman-compose is not on PATH.
  Install it with:   pip install --user podman-compose
  (Do not substitute docker-compose — see the note at the top of this script.)
EOF
  exit 1
fi

# Rootless podman needs no daemon, but it does need its storage tree to exist.
# A missing storage/tmp is a known failure mode and the error it produces
# ("creating a temporary directory: ... no such file or directory") is opaque,
# so create it rather than making the next command fail mysteriously.
storage_root="${XDG_DATA_HOME:-$HOME/.local/share}/containers/storage"
mkdir -p "$storage_root/tmp"

if ! podman info >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: `podman info` failed — the container runtime is not usable.
  For rootless podman, check:
    podman info
    systemctl --user status podman.socket   # only needed for the docker-compat socket
EOF
  exit 1
fi

if ! podman ps >/dev/null 2>&1; then
  echo "ERROR: 'podman ps' failed — podman is installed but not answering." >&2
  exit 1
fi

echo "container runtime OK — podman $(podman --version | awk '{print $3}')," \
     "podman-compose $(podman-compose --version 2>/dev/null | awk '/podman-compose version/{print $3}')," \
     "rootless=$(podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null)"
