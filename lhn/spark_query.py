
from lhn.spark_utils import writeTable
from lhn.header import StringIndexer, OneHotEncoder, spark, F, DataFrame, np 
from lhn.header import get_logger
# Get the logger configured in __init__.py
logger = get_logger(__name__)
 
def clean_values(column):
    """Modify values of a column so they can be used as veriable names in a pivot wider transformation
    
    Args:
        column (_type_): Name of a column in a data table

    Returns:            
        _type_: a columns
    """
    # Remove special characters
    cleaned_column = F.regexp_replace(column, "[^\\w\\s]", "")
    # Replace spaces with underscores
    cleaned_column = F.regexp_replace(cleaned_column, "\\s+", "_")
    return cleaned_column


def stackedSpark(df, cols, names_to, values_to, values_drop_na=True):
    """
    Convert a wide Spark DataFrame into a long format.

    This function is similar to the pivot_longer function from the tidyr package in R. It allows for repeated levels of index fields in the result if they are each indicated in one of the indicator fields.

    Parameters:
    df (DataFrame): The input Spark DataFrame in wide format.
    cols (list): The columns to pivot into a longer format.
    names_to (str): The name of the new column storing the column names specified by `cols`.
    values_to (str): The name of the new column storing the value in the column names specified by `cols`.
    values_drop_na (bool, optional): If True, will drop rows that contain only NAs in the `values_to` column. Defaults to True.

    Returns:
    DataFrame: The DataFrame in long format.

    Example:
    longdf = stackedSpark(
        e.arrhythmia.df, 
        cols=[item for item in e.arrhythmia.df.columns if item not in [*e.arrhythmia.indexFields, 'Subjects']],
        names_to="disease",
        values_to='selected'
    )
    """    
    
    index_fields = [item for item in df.columns if item not in cols]
    stack_expr = "stack(" + str(len(cols)) + ", " + ", ".join(["'{0}', {0}".format(x) for x in cols]) + ")"
    df_long = df.selectExpr(*index_fields, stack_expr).withColumnRenamed("col0", names_to).withColumnRenamed("col1", values_to)
    df_long = df_long.filter(F.col(values_to) == 1.0)
    if values_drop_na:
        df_long = df_long.filter(F.col(values_to).isNotNull())
    return(df_long)
    
    
def pivot_wider(df, id_cols, names_from, values_from, hotone=False, function=None, aggfunc=np.mean, df_type='pandas', values_fill=0):
    """
    Pivot a dataframe from long to wide format.
    
    Args              : 
        df            : dataframe to pivot
        id_cols       : list of columns to use as id variables
        names_from    : str,  the column to use for the new variable names
        values_from   : str,  the column to use for the new values
        hotone        : flag to use hot-one encoding (default: False)
        function      : the function to use to aggregate the values for Spark DataFrame (default: None)
        aggfunc       : the function to use to aggregate the values for Pandas DataFrame (default: np.mean)
        df_type       : the type of the input dataframe ('pandas' or 'spark')
        values_fill   : value to fill missing values after pivoting (default: 0)
    
    Returns:
        dataframe in wide format
    """
    
    if df_type == 'spark':
        if hotone:
            string_cols = id_cols + [names_from]
            for col_name in string_cols:
                indexer = StringIndexer(inputCol=col_name, outputCol=col_name+"_index")
                df = indexer.fit(df).transform(df)

            encoder = OneHotEncoder(inputCols=[names_from+"_index"], outputCols=[names_from+"_hotencode"])
            df = encoder.fit(df).transform(df)

            drop_cols = [col_name+"_index" for col_name in string_cols] + [names_from]
            df = df.drop(*drop_cols)

        pivot_vals = function(values_from)
        pivot_spec = {name: "sum" for name in pivot_vals}
        pivot_cols = id_cols + [names_from+"_hotencode"] if hotone else id_cols + [names_from]

        df = df.groupBy(*pivot_cols).agg(pivot_spec)

        for col_name in id_cols:
            df = df.withColumnRenamed(col_name, "{}_new".format(col_name))

        if hotone:
            df = df.withColumnRenamed(names_from+"_hotencode", names_from)

        result = df.groupby([F.col("{}".format(col_name+"_new")) for col_name in id_cols]).pivot(names_from).agg(pivot_vals).fillna(values_fill)

    elif df_type == 'pandas':
        result = df[id_cols + [names_from, values_from]].drop_duplicates()
    
        if hotone:
            result = result.astype({names_from: str, values_from: int})
            result[names_from] = result[names_from].astype(str) + '_' + result[values_from].astype(str)
            result.drop(columns=[values_from], inplace=True)
            result = result.pivot_table(index=id_cols, columns=names_from, aggfunc=aggfunc)
            result.columns = result.columns.map(lambda x: x[1] if isinstance(x, tuple) else x)
        else:
            result = result.pivot_table(index=id_cols, columns=names_from, values=values_from, aggfunc=aggfunc)
            
        result.reset_index(inplace=True)
        result.sort_values(id_cols, inplace=True)
        result.columns.name = None

    else:
        raise ValueError("df_type must be either 'pandas' or 'spark'")
    
    return result

def joinByIndex(DF1, DF2, index1, index2, cols1, cols2, outIndex, outTable, how = 'inner', messages = False):
    """
    Use indexes to join two tables together, The resulting table will have the union of index1 and index2.
    DF1 Should be the small set and DF2 The larger table.
    """
    
    # The intersection of the indexes will have these columns
    
    index = []
    [index.append(item) for item in index1 if item in index2 and item not in index]
    
    # Remove the columns in the second table that are duplicated in the first
    cols2a = []
    [cols2a.append(item) for item in cols2 if (item not in cols1 or item in index) ]
    
    # Broadcast the index of the smaller table
    DF1.select(index).createOrReplaceTempView('DF1Index')
    DF1Index = F.broadcast(spark.table('DF1Index'))
    
    # Identify the index of the larger table
    DF2.select(index).createOrReplaceTempView('DF2Index')
    DF2Index = spark.table('DF2Index')
    
    # Join just the indexes, should be fast
    (
        DF2Index
        .join(DF1Index, on = index, how = how)
        .distinct()
        .write
        .saveAsTable(outIndex, mode = 'overwrite')
    )
    DFIndex = spark.table(outIndex)
    if messages:
        print(f"created {outIndex}")
    
    # Only the rows selected from table 1
    DF1I = (
        DF1
        .select(cols1)
        .join(DFIndex, on = index, how = how)
        .createOrReplaceTempView('DF1I')
    )
    DF1I = spark.table('DF1I')
    
    # only the rows selected of table 2
    (
        DF2
        .select(cols2a)
        .join(DFIndex, on = index, how = how)
        .createOrReplaceTempView('DF2IndexI')
    )
    DF2I = spark.table('DF2IndexI')
    
    # The final table is saved
    DFout = (
        DF2I
        .join(DF1I, on = index, how = how)
        .distinct()
        .write
        .saveAsTable(outTable, mode = 'overwrite')
        
    )



def findElementsNotUsed(indexDF, entityIndex, sourceEncounter, elementSource, elementIndex, outEncounterTBL, outElementTBL):
    """
    Given a set of found encounters, a masterlist of encounters IDs and a table of source elements such as labs, 
    find the labs in the table of encounter IDs that did not appear in the indexDF
    @param indexDF: A data frame with each of the included encounter index
    @param entityIndex: The index of the encounters, such as personid
    @param sourceEncounter: a table of all possible encounters to be considered
    @param elementSource: A list of all source elements, such as lab records
    @param elementIndex, the index of an encounter, such as personid, encounterid
    @param outEncounterTBL: Name of the table listing the encounters not included in indexDF but in encounterSource
    @param outElementTBL: The resulting list of encounters, such as lab encounters, not included in indexDF.
    """

    # Create a list of unique levels of entityIndex from indexDF
    
    indexDF.select(entityIndex).distinct().createOrReplaceTempView('indexDF')
    
    # Find levels of column elementIndex not in the indexDF
    noEnc = (
        sourceEncounter
        .join(spark.table('indexDF'), on = indexDF, how = 'leftanti')
        .select(elementIndex)
        .distinct()
    )

    # write because the is a large calculation. 
    writeTable(noEnc, outEncounterTBL)
    
    # Get the records from the Source not previously in indexDF
    noElements = (
        elementSource
        .join(spark.table(outEncounterTBL), on = elementIndex, how = 'inner')
    )

    writeTable(noElements, outElementTBL)
    
    return(spark.table(outElementTBL))
    



def add_parsed_column(data: DataFrame, primaryDisplay: str, elementList: list = None, value_column: str = "group") -> DataFrame:
    """
    This function takes a PySpark DataFrame, searches for a column primaryDisplay, and applies
    a list of regular expression matches to it. If a match is found, the corresponding regex
    group name from a provided list elementList is used to create a new column with the name
    specified in the value_column parameter.
    
    :param data: PySpark DataFrame
    :param primaryDisplay: Name of column in data to parse
    :param elementList: List of regular expressions to match in primaryDisplay
    :param value_column: Name of new column for matched regex groups
    :return: PySpark DataFrame with new parsed column
    """
    if elementList:
        data = data.withColumn(value_column, F.expr("''"))
        for i, pattern in enumerate(elementList):
            data = data.withColumn(value_column,
                                   F.when(F.col(primaryDisplay).rlike(f'(?i){pattern}'), f'{elementList[i]}')
                                   .otherwise(F.col(value_column)))
        data = data.withColumn(value_column, F.col(value_column).substr(1, 1000)) # remove the 'u'
    else:
        raise Exception('elementList is empty')
    return data



