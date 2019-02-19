def normalize_column_names(df):
    """
        Written specifically for this application, as some records had column names with
        different casing, if the column names are similar the values are copied to column names with lowercasing
        :param df: any pandas.DataFrame
        :return: DataFrame with the names normalized
    """
    from collections import defaultdict
    d = defaultdict(list)
    col_names = df.columns.values
    for col in col_names:
        d[col.lower()].append(col)
    for i in d:
        if len(d[i]) == 2:
            df[i.lower()] = df[[d[i][0], d[i][1]]].apply(get_non_null_column, axis=1)
            df.drop([d[i][1]], axis=1)
    return df


def normalise_name(string):
    """

    :param string: any string
    :return: string with all the " " trimmed
    """
    return string.replace(" ", "")


def get_non_null_column(cols):
    """
        Given 2 columns returns the column with non null value if available
        :param cols
        :return: non-null value if available
    """
    if not isinstance(cols[1], float):
        return cols[1]
    return cols[0]
