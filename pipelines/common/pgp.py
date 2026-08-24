"""Resolve a ready-to-use GPG keyring for the file processor.

Two worlds, one call site:

* **local** (DirectRunner, `make smoke`, `make smoke-gcp`): the throwaway prototype
  keypair lives on disk at `cfg.pgp_home` (see `local/scripts/gen_pgp_key.sh`). The GPG
  home directory is used directly — nothing to materialise.

* **real GCP** (DataflowRunner, Flex Template): the private key and passphrase are in
  Secret Manager (`pgp-private-key`, `pgp-passphrase` — see
  `terraform/modules/secrets/main.tf` and `make seed-secrets`). A Dataflow worker has no
  on-disk keyring, so the key is fetched once per worker process and written into a
  private temp gnupg home before the first decrypt. `python-gnupg` then operates on that
  home exactly as it does locally.

The seam is `resolve_gpg(cfg)`; the caller does not know which world it is in.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import gnupg

from .config import Config
from .storage import Secrets


def resolve_gpg(cfg: Config) -> Any:
    """Return a `gnupg.GPG` instance with the migration private key loaded.

    Locally the existing keyring at `cfg.pgp_home` is used as-is. On real GCP the key
    material is pulled from Secret Manager into a fresh temp home. The temp home is
    per-process; `save_main_session=True` keeps the returned object alive for the worker
    lifetime, and the directory itself is left in place (the worker is ephemeral).
    """
    if cfg.is_local:
        return gnupg.GPG(gnupghome=cfg.pgp_home)

    home = tempfile.mkdtemp(prefix="mig-gnupg-")
    # gnupg refuses to operate on a world-readable home.
    os.chmod(home, 0o700)
    secrets = Secrets(cfg)
    private_key = secrets.get("pgp-private-key")
    passphrase = secrets.get("pgp-passphrase").decode("utf-8").strip()
    gpg = gnupg.GPG(gnupghome=home)
    # python-gnupg mangles the import when handed an empty passphrase (gpg sees no
    # OpenPGP data at all) — only pass one when the key is actually protected.
    if passphrase:
        import_result = gpg.import_keys(private_key.decode("utf-8"), passphrase=passphrase)
    else:
        import_result = gpg.import_keys(private_key.decode("utf-8"))
    if not import_result.count:
        # ImportResult has no `.status`; stderr carries gpg's actual reason.
        detail = (getattr(import_result, "stderr", "") or "no key imported").strip()
        raise RuntimeError(
            "failed to import the PGP private key from Secret Manager "
            f"(pgp-private-key): {detail[:500]}"
        )
    # python-gnupg holds passphrase out-of-band; pass it on each decrypt instead via the
    # caller. We expose it on the object for the caller's convenience.
    gpg.mig_passphrase = passphrase or None  # type: ignore[attr-defined]
    return gpg