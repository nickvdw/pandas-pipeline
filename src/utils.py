def log_step(func):
    """
    Method used for logging each step in the pandas pipeline.
    Wraps each pipeline step and prints the time and shape associated with each step.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = dt.datetime.now()
        result = func(*args, **kwargs)
        time_taken = str(dt.datetime.now() - tic)
        print(f"just ran step {func.__name__} shape={result.shape} took {time_taken}s")
        return result
    return wrapper

@log_step
def start_pipeline(dataf):
    """
    Creates a copy of the dataframe in order to not modify the initial dataframe.
    """
    return dataf.copy() 

@log_step
def set_dtypes(dataf):
    """
    Sets the dtypes of the columns (and sorts).
    """
    return (dataf
            .assign(date=lambda d: pd.to_datetime(d['date']))
            .sort_values(['currency_code', 'date']))

@log_step
def add_inflation_features(dataf):
    """
    Add new features to the pipeline.
    """
    return (dataf
            .assign(local_inflation=lambda d: d.groupby('name')['local_price'].diff()/d['local_price'])
            .assign(dollar_inflation=lambda d: d.groupby('name')['dollar_price'].diff()/d['dollar_price']))

@log_step
def remove_outliers(dataf, min_row_country=32):
    """
    Remove outliers based on a certain threshold that can be passed as an argument in the pipeline.
    """
    countries = (dataf
                .groupby('currency_code')
                .agg(n=('name', 'count'))
                .loc[lambda d: d['n'] >= min_row_country]
                .index)
    return (dataf
            .loc[lambda d: d['currency_code'].isin(countries)]
            .loc[lambda d: d['local_inflation'] > -20])

def plot_bigmac(dataf):
    """
    Plot certain aspects of the dataframe.
    """
    return (alt.Chart(dataf)
      .mark_point()
      .encode(x='local_inflation', 
              y='dollar_inflation', 
              color=alt.Color('currency_code'),
              tooltip=["currency_code", "local_inflation", "dollar_inflation"])
      .properties(width=600, height=150)
      .interactive())
  