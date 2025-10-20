from pathlib import Path

def test_path_objects_are_stringified_consistently(use_real_asset_init, tmp_path):
    Asset = use_real_asset_init

    class DummyAsset(Asset):
        def get(self):
            return None

    # Simulate Windows-like path segments; Path will normalize for current OS,
    # but hashing should be equivalent if given as Path versus str(Path)
    win_style_path = tmp_path / "Some Folder" / "nested" / "file.txt"
    inputs_path_obj = {"data_path": Path(win_style_path)}
    inputs_str = {"data_path": str(win_style_path)}

    a = DummyAsset(inputs=inputs_path_obj)
    b = DummyAsset(inputs=inputs_str)

    assert a.inputs_hash == b.inputs_hash, "Path objects must be serialized to strings for hashing"
