from pathlib import Path
from mobility.runtime.assets.asset import Asset

def test_path_objects_are_stringified_consistently(tmp_path):
    class DummyAsset(Asset):
        def get(self):
            return None

    # Path inputs should hash deterministically, but remain distinct from raw
    # strings now that the hash payload preserves input types explicitly.
    win_style_path = tmp_path / "Some Folder" / "nested" / "file.txt"
    inputs_path_obj = {"data_path": Path(win_style_path)}
    inputs_str = {"data_path": str(win_style_path)}

    a = DummyAsset(inputs=inputs_path_obj)
    a_again = DummyAsset(inputs=inputs_path_obj)
    b = DummyAsset(inputs=inputs_str)

    assert a.inputs_hash == a_again.inputs_hash
    assert a.inputs_hash != b.inputs_hash
