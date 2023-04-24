import pandas as pd

# /!\ A default minimum_sample_size remains to be determined


def safe_sample(
    data_base, n_sample, weights="pondki", minimum_sample_size=10, **kwargs
):
    """
    Samples the data base filtered by kwargs.

    Handles the case where the sample size is lesser than minimum_sample_size by withdrawing the filters

    Args:
        data_base (pd.DataFrame):
            The database to sample from. Must be indexed (or muli-indexed) by the keys of kwargs.
        n_sample (int):
            The number of samples to draw.
        weights (str) :
            The name of columns of data_base containing the weights for the sampling.
        minimum_sample_size (int) :
            The minimum size of the database to draw from.
            If the kwargs make the database to small, relax the criteria from last to first.
        kwargs :
            the criteria to filter the database

    Returns:
        pd.DataFrame: a dataframe with n_sample rows.

    Example :
        safe_sample(days_trip_db, 10, csp="3", n_cars="2+", weekday=True, city_category='C')
    """
    # Filter by the kwargs
    for key in kwargs.keys():
        sample_size = (data_base.index.get_level_values(key) == kwargs[key]).sum()
        if sample_size < minimum_sample_size:
            # Sample size too small -> Relax the current criteria
            data_base.reset_index(level=key, inplace=True)
            # print('The '+key+' criteria has been relaxed.')

        else:
            if isinstance(data_base.index, pd.MultiIndex):
                data_base = data_base.xs(kwargs[key], level=key)

            else:
                data_base = data_base.xs(kwargs[key])

    if type(data_base) == pd.Series:
        # The database to sample from is just one row
        data_base = pd.DataFrame([data_base])

    return data_base.sample(n_sample, weights=weights, replace=True, axis=0)
