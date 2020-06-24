# code Eli B on StackOverflow
# obtained from https://stackoverflow.com/questions/39758045/how-to-perform-union-on-two-dataframes-with-different-amounts-of-columns-in-spar


from pyspark.sql.functions import lit 

def __order_df_and_add_missing_cols(df, columns_order_list, df_missing_fields):
    """ return ordered dataFrame by the columns order list with null in missing columns """
    if not df_missing_fields:  # no missing fields for the df
        return df.select(columns_order_list)
    else:
        columns = []
        for colName in columns_order_list:
            if colName not in df_missing_fields:
                columns.append(colName)
            else:
                columns.append(lit(None).alias(colName))
        return df.select(columns)


def __add_missing_columns(df, missing_column_names):
    """ Add missing columns as null in the end of the columns list """
    list_missing_columns = []
    for col in missing_column_names:
        list_missing_columns.append(lit(None).alias(col))

    return df.select(df.schema.names + list_missing_columns)


def __order_and_union_d_fs(left_df, right_df, left_list_miss_cols, right_list_miss_cols):
    """ return union of data frames with ordered columns by left_df. """
    left_df_all_cols = __add_missing_columns(left_df, left_list_miss_cols)
    right_df_all_cols = __order_df_and_add_missing_cols(right_df, left_df_all_cols.schema.names,
                                                        right_list_miss_cols)
    return left_df_all_cols.union(right_df_all_cols)


def union_d_fs(left_df, right_df):
    """ Union between two dataFrames, if there is a gap of column fields,
     it will append all missing columns as nulls """
    # Check for None input
    if left_df is None:
        raise ValueError('left_df parameter should not be None')
    if right_df is None:
        raise ValueError('right_df parameter should not be None')
        # For data frames with equal columns and order- regular union
    if left_df.schema.names == right_df.schema.names:
        return left_df.union(right_df)
    else:  # Different columns
        # Save dataFrame columns name list as set
        left_df_col_list = set(left_df.schema.names)
        right_df_col_list = set(right_df.schema.names)
        # Diff columns between left_df and right_df
        right_list_miss_cols = list(left_df_col_list - right_df_col_list)
        left_list_miss_cols = list(right_df_col_list - left_df_col_list)
        return __order_and_union_d_fs(left_df, right_df, left_list_miss_cols, right_list_miss_cols)