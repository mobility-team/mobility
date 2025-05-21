import numpy as np
import random

def sample_travels(df, start_col, length_col, weight_col, k,
                                      burnin=10000, thinning=1000, num_samples=1,
                                      random_seed=None):
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
    if random_seed is not None:
        random.seed(random_seed)
        np.random.seed(random_seed)

    n = len(df)
    indices = np.arange(n)
    starts = df[start_col].to_numpy()
    ends = starts + df[length_col].to_numpy()
    weights = df[weight_col].to_numpy()

    def is_compatible(v, sample_idxs):
        """Vectorized check: interval v vs. intervals in sample_idxs."""
        sv, ev = starts[v], ends[v]
        s = starts[sample_idxs]
        e = ends[sample_idxs]
        return np.all((ev <= s) | (sv >= e))

    # 1. Initialize S with a random-greedy valid set
    perm = np.random.permutation(n)
    S = []
    for idx in perm:
        if len(S) < k and is_compatible(idx, S):
            S.append(idx)
    if len(S) < k:
        raise ValueError("Initialization failed: cannot find size-k non-overlapping set")

    all_set = set(indices)
    samples = []
    total_steps = burnin + thinning * num_samples

    # 2. MCMC loop with weighted MH swap moves
    for t in range(total_steps):
        # pick a member u to remove, and a candidate v to add
        u = random.choice(S)
        v = random.choice(list(all_set - set(S)))
        # check non-overlap if we swap u->v
        S_without_u = [x for x in S if x != u]
        if is_compatible(v, S_without_u):
            # compute MH acceptance ratio
            ratio = weights[v] / weights[u]
            if random.random() < min(1, ratio):
                # accept swap
                S[S.index(u)] = v

        # record sample after burn-in and respecting thinning
        if t >= burnin and (t - burnin) % thinning == 0:
            samples.append(list(S))

    # 3. Map back to original DataFrame indices and return
    df_index = df.index.to_list()
    return [[df_index[i] for i in sample] for sample in samples]

