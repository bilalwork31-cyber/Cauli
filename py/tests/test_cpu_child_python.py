"""Which interpreter the worker spawns cpu children with.

An embedded CPython has no argv[0] of its own, so it resolves `sys.executable`
by looking up "python3" on PATH. When the libpython the worker links is not
that interpreter (a uv or pyenv Python, a free threaded build), the children
ran a different CPython and could not import the app's C extensions.
"""

from __future__ import annotations

import os
import sys
import sysconfig
from pathlib import Path

import pytest

OWN_NAME = f"python{sys.version_info.major}.{sys.version_info.minor}{sys.abiflags}"


def _own_interpreter() -> str:
    path = os.path.join(sysconfig.get_config_var("BINDIR"), OWN_NAME)
    if not os.path.isfile(path):
        pytest.skip(f"{path} not present in this interpreter's BINDIR")
    return path


def _venv_over(base: str, root: Path) -> str:
    (root / "bin").mkdir(parents=True)
    (root / "pyvenv.cfg").write_text(f"home = {os.path.dirname(base)}\n")
    python = root / "bin" / "python"
    python.symlink_to(base)
    return str(python)


def test_a_foreign_python3_on_path_is_not_used(shim, tmp_path, monkeypatch):
    own = _own_interpreter()
    foreign = tmp_path / "python3"
    foreign.write_text("")
    monkeypatch.setattr(sys, "executable", str(foreign))
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    assert shim.cpu_child_python() == own


def test_the_venv_the_launcher_exported_is_preferred(shim, tmp_path, monkeypatch):
    own = _own_interpreter()
    venv_python = _venv_over(own, tmp_path / "venv")
    foreign = tmp_path / "python3"
    foreign.write_text("")
    monkeypatch.setattr(sys, "executable", str(foreign))
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / "venv"))
    assert shim.cpu_child_python() == venv_python


def test_a_venv_over_a_different_python_is_not_used(shim, tmp_path, monkeypatch):
    own = _own_interpreter()
    other_base = tmp_path / "other" / "bin" / OWN_NAME
    other_base.parent.mkdir(parents=True)
    other_base.write_text("")
    _venv_over(str(other_base), tmp_path / "venv")
    monkeypatch.setattr(sys, "executable", str(tmp_path / "venv" / "bin" / "python"))
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / "venv"))
    assert shim.cpu_child_python() == own


def test_the_venv_wins_when_sys_executable_is_its_own_base_interpreter(
    shim, tmp_path, monkeypatch
):
    """A venv over the system python, never activated: an embedded
    sys.executable is the system python3 off PATH, which lacks the venv's
    packages."""
    own = _own_interpreter()
    venv_python = _venv_over(own, tmp_path / "venv")
    monkeypatch.setattr(sys, "executable", own)
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / "venv"))
    assert shim.cpu_child_python() == venv_python


def test_sys_executable_is_kept_when_it_is_this_interpreter(shim, monkeypatch):
    own = _own_interpreter()
    monkeypatch.setattr(sys, "executable", own)
    assert shim.cpu_child_python() == own
