# From Marsaglia's Fast Generation of Discrete Random Variables
# https://www.jstatsoft.org/article/view/v011i03
def get_5tbls(P, offset=0):
    
    # Convert to 30-bit fixed-point integers
    P = np.asarray(P)
    P = np.round(P * (1 << 30)).astype(np.uint32)
    size = len(P)

    # Vectorized base-64 digit extraction
    def dg_all(P, k): return (P >> (30 - 6 * k)) & 0x3F
    dg1, dg2, dg3, dg4, dg5 = [dg_all(P, k) for k in range(1, 6)]

    # Precompute total lengths
    AA = np.repeat(np.arange(size) + offset, dg1)
    BB = np.repeat(np.arange(size) + offset, dg2)
    CC = np.repeat(np.arange(size) + offset, dg3)
    DD = np.repeat(np.arange(size) + offset, dg4)
    EE = np.repeat(np.arange(size) + offset, dg5)

    # Thresholds
    t1 = len(AA) << 24
    t2 = t1 + (len(BB) << 18)
    t3 = t2 + (len(CC) << 12)
    t4 = t3 + (len(DD) << 6)

    return {
        'AA': AA, 'BB': BB, 'CC': CC, 'DD': DD, 'EE': EE,
        't1': t1, 't2': t2, 't3': t3, 't4': t4
    }


from numpy.random import default_rng
rng = default_rng(seed=1234)

def dran(rng, tables):
    j = rng.integers(0, 1 << 30, dtype=np.uint32)
    t = tables
    if j < t['t1']:
        return t['AA'][j >> 24]
    if j < t['t2']:
        return t['BB'][(j - t['t1']) >> 18]
    if j < t['t3']:
        return t['CC'][(j - t['t2']) >> 12]
    if j < t['t4']:
        return t['DD'][(j - t['t3']) >> 6]
    return t['EE'][j - t['t4']]

def dran_many(rng, tables, n):
    j = rng.integers(0, 1 << 30, size=n, dtype=np.uint32)
    out = np.empty(n, dtype=np.int32)

    t1, t2, t3, t4 = tables["t1"], tables["t2"], tables["t3"], tables["t4"]

    mask1 = j < t1
    mask2 = (j >= t1) & (j < t2)
    mask3 = (j >= t2) & (j < t3)
    mask4 = (j >= t3) & (j < t4)
    mask5 = j >= t4

    out[mask1] = tables["AA"][j[mask1] >> 24]
    out[mask2] = tables["BB"][(j[mask2] - t1) >> 18]
    out[mask3] = tables["CC"][(j[mask3] - t2) >> 12]
    out[mask4] = tables["DD"][(j[mask4] - t3) >> 6]
    out[mask5] = tables["EE"][j[mask5] - t4]

    return out

p = cost_bin_to_dest.filter(pl.col("motive") == "work").filter(pl.col("from") == 479).filter(pl.col("cost_bin") == 1.0)["p_to"]
tbls = get_5tbls(p)
dran(rng, tbls)
dran_many(rng, tbls, 100)