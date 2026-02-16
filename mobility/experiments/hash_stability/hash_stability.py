import json, platform, sys
import polars as pl

SEED = 12345

# Replace this with your real df if you want:
df = pl.DataFrame(
    {
        "demand_group_id": pl.Series([13, 25, 16, 16, 9, 2029, 2028, 2032, 2030, 2029], dtype=pl.UInt32),
        "home_zone_id": pl.Series([1, 1, 1, 1, 1, 77, 77, 77, 77, 77], dtype=pl.Int32),
        "motive_seq_id": pl.Series([241, 241, 215, 228, 143, 237, 235, 227, 215, 241], dtype=pl.UInt32),
        "motive": pl.Series(
            ["work"] * 10,
            dtype=pl.Enum(["home", "other", "studies", "work"]),
        ),
        "to": pl.Series([76, 76, 76, 76, 76, 63, 63, 63, 63, 63], dtype=pl.Int32),
        "p_ij": pl.Series([0.185129]*5 + [0.010314]*5, dtype=pl.Float64),
    }
)

hashes = (
    df.select(
        pl.struct(["demand_group_id", "motive_seq_id", "motive", "to"])
        .hash(seed=SEED)
        .alias("h")
    )["h"]
    .to_list()
)

payload = {
    "polars_version": pl.__version__,
    "python_version": sys.version.split()[0],
    "machine": platform.machine(),
    "platform": platform.platform(),
    "seed": SEED,
    "hashes": hashes,
}

print("CURRENT:")
print(json.dumps(payload, indent=2))