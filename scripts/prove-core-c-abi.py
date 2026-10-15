#!/usr/bin/env python3
"""Build the shipped c-shared artifact and invoke it as a foreign consumer."""

from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    evidence_value = os.environ.get("LAZILY_CORE_EVIDENCE_PATH")
    if not evidence_value:
        raise SystemExit("LAZILY_CORE_EVIDENCE_PATH is required")
    evidence_path = Path(evidence_value)
    with tempfile.TemporaryDirectory(prefix="lazily-go-core-abi-") as temporary:
        artifact = Path(temporary) / "liblazily_go.so"
        environment = dict(os.environ)
        environment["CGO_ENABLED"] = "1"
        subprocess.run(
            ["go", "build", "-buildmode=c-shared", "-o", str(artifact), "./cmd/lazily-ffi-shared"],
            cwd=ROOT,
            env=environment,
            check=True,
        )
        library = ctypes.CDLL(str(artifact))
        library.lazily_ffi_channel_new.restype = ctypes.c_size_t
        library.lazily_ffi_channel_len.argtypes = [ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
        library.lazily_ffi_channel_len.restype = ctypes.c_int
        library.lazily_ffi_channel_free.argtypes = [ctypes.c_size_t]
        handle = library.lazily_ffi_channel_new()
        if handle == 0:
            raise SystemExit("native channel constructor returned a null handle")
        length = ctypes.c_size_t(99)
        status = library.lazily_ffi_channel_len(handle, ctypes.byref(length))
        library.lazily_ffi_channel_free(handle)
        if status != 0 or length.value != 0:
            raise SystemExit(f"native channel length returned status={status}, length={length.value}")

    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.write_text(
        json.dumps(
            {
                "status": "passed",
                "claim": os.environ.get("LAZILY_CORE_CLAIM_ID"),
                "artifact": "go build -buildmode=c-shared ./cmd/lazily-ffi-shared",
                "consumer": "Python ctypes",
                "symbols_invoked": [
                    "lazily_ffi_channel_new",
                    "lazily_ffi_channel_len",
                    "lazily_ffi_channel_free",
                ],
                "observed": {"status": status, "length": length.value},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"C ABI proof failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
