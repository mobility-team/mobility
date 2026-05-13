import json
import os
from pathlib import Path

import pytest

from mobility.config import (
    apply_import_time_memory_reclaim_policy,
    update,
)


def test_update_uses_default_package_data_folder(tmp_home):
    with pytest.warns(UserWarning):
        config_path = update(memory_reclaim_policy="default")

    assert config_path == Path(tmp_home) / ".mobility" / "mobility_config.json"
    assert config_path.exists()
    assert json.loads(config_path.read_text(encoding="utf-8")) == {"memory_reclaim_policy": "default"}


def test_update_uses_stable_user_config_path_even_if_package_data_exists(tmp_home, tmp_path):
    (tmp_path / "custom_pkg_data").mkdir(parents=True, exist_ok=True)
    with pytest.warns(UserWarning, match="Restart Python or your notebook kernel"):
        written_path = update(memory_reclaim_policy="default")

    assert written_path == Path(tmp_home) / ".mobility" / "mobility_config.json"
    assert written_path.exists()
    assert json.loads(written_path.read_text(encoding="utf-8")) == {"memory_reclaim_policy": "default"}


def test_update_preserves_existing_config_keys(tmp_home):
    config_path = Path(tmp_home) / ".mobility" / "mobility_config.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps({"foo": "bar"}), encoding="utf-8")

    with pytest.warns(UserWarning):
        update(memory_reclaim_policy="aggressive")

    assert json.loads(config_path.read_text(encoding="utf-8")) == {
        "foo": "bar",
        "memory_reclaim_policy": "aggressive",
    }


def test_update_rejects_unknown_memory_reclaim_policy(tmp_path):
    with pytest.raises(ValueError, match="Unknown memory reclaim policy"):
        update(memory_reclaim_policy="fast")


def test_apply_import_time_memory_reclaim_policy_uses_default_when_file_is_missing(tmp_home):
    policy = apply_import_time_memory_reclaim_policy()

    assert policy == "aggressive"


@pytest.mark.parametrize(
    "system_name, policy_value, env_key, expected_env_value",
    [
        ("Windows", "aggressive", "MIMALLOC_PURGE_DELAY", "0"),
        ("Linux", "aggressive", "_RJEM_MALLOC_CONF", "dirty_decay_ms:0,muzzy_decay_ms:0"),
        ("Windows", "default", "MIMALLOC_PURGE_DELAY", None),
    ],
)
def test_apply_import_time_memory_reclaim_policy_sets_expected_allocator_env(
    monkeypatch,
    system_name,
    policy_value,
    env_key,
    expected_env_value,
):
    monkeypatch.setattr("platform.system", lambda: system_name, raising=True)

    with pytest.warns(UserWarning):
        update(memory_reclaim_policy=policy_value)

    policy = apply_import_time_memory_reclaim_policy()

    assert policy == policy_value
    if expected_env_value is None:
        assert env_key not in os.environ
    else:
        assert os.environ[env_key] == expected_env_value


def test_apply_import_time_memory_reclaim_policy_does_not_override_existing_allocator_env(monkeypatch):
    monkeypatch.setattr("platform.system", lambda: "Windows", raising=True)
    monkeypatch.setenv("MIMALLOC_PURGE_DELAY", "123")

    with pytest.warns(UserWarning):
        update(memory_reclaim_policy="aggressive")

    apply_import_time_memory_reclaim_policy()

    assert os.environ["MIMALLOC_PURGE_DELAY"] == "123"
