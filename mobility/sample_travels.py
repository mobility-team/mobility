import numpy as np
import random

def _max_non_overlap_k(starts, ends):
    """
    Compute the maximum number of non-overlapping intervals.
    Uses the classic interval‐scheduling greedy: sort by end time, then pick.
    """
    # sort indices by their end times (earliest-ending first)
    idx = np.argsort(ends)
    last_end = -np.inf
    count = 0

    for i in idx:
        # if this interval starts after or exactly when the last one ended, we can take it
        if starts[i] >= last_end:
            count += 1
            last_end = ends[i]

    return count


def sample_travels(
    df,
    start_col,
    length_col,
    weight_col,
    k,
    burnin=10000,
    thinning=1000,
    num_samples=1,
    random_seed=None
):
    """
    Weighted MCMC sampling of k non-overlapping travels via swap-move Metropolis-Hastings.

    Parameters:
    - df: pandas DataFrame with travel records.
    - start_col: column name for integer start index (e.g., day-of-year).
    - length_col: column name for duration (number of nights).
    - weight_col: column name for the base survey weight of each travel.
    - k: desired sample size.
    - burnin: number of initial MCMC steps to discard.
    - thinning: number of steps between recorded samples.
    - num_samples: how many independent samples to collect.
    - random_seed: for reproducibility.

    Returns:
    - List of `num_samples` lists of DataFrame indices.
    """
    # 0) Optional: set random seeds for reproducibility
    if random_seed is not None:
        random.seed(random_seed)
        np.random.seed(random_seed)

    # 1) Extract helper arrays from the DataFrame
    n = len(df)
    indices = np.arange(n)
    starts = df[start_col].to_numpy()
    # compute end day = start day + duration
    ends = starts + df[length_col].to_numpy()
    weights = df[weight_col].to_numpy()

    # 2) Cap k to the true maximum non-overlapping set size
    #    Ensures it's always possible to find k intervals
    K_max = _max_non_overlap_k(starts, ends)
    if k > K_max:
        k = K_max

    # 3) Deterministic greedy initialization
    #    Always succeeds for k ≤ K_max
    idx_by_end = np.argsort(ends)  # intervals sorted by end time
    S = []                         # will hold chosen indices
    last_end = -np.inf

    for i in idx_by_end:
        # if interval i doesn't overlap the last picked one, take it
        if starts[i] >= last_end and len(S) < k:
            S.append(i)
            last_end = ends[i]
            
    if len(S) == 0:
        return [[]]

    # 4) Prepare for the MCMC swap moves
    all_set = set(indices)
    samples = []
    total_steps = burnin + thinning * num_samples

    def is_compatible(v, sample_idxs):
        """
        Check if interval v does not overlap any in sample_idxs.
        Vectorized using NumPy.
        """
        sv, ev = starts[v], ends[v]
        s = starts[sample_idxs]
        e = ends[sample_idxs]
        # no overlap if v ends before all start, or starts after all end
        return np.all((ev <= s) | (sv >= e))

    # 5) MCMC loop: at each step, propose swapping one chosen interval for an outside one
    for t in range(total_steps):
        # pick a random element u from current set S to remove
        u = random.choice(S)
        # pick a random candidate v from outside S to potentially add
        v = random.choice(list(all_set - set(S)))

        # only consider swap if v is compatible with S without u
        if is_compatible(v, [x for x in S if x != u]):
            ratio = weights[v] / weights[u]             # weight ratio
            if random.random() < min(1, ratio):         # MH accept/reject
                S[S.index(u)] = v                       # accept swap

        # after burn-in and at thinning intervals, record the sample
        if t >= burnin and (t - burnin) % thinning == 0:
            samples.append(list(S))

    # 6) Map back from integer positions to DataFrame indices
    df_index = df.index.to_list()
    return [[df_index[i] for i in sample] for sample in samples]
