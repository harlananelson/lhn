
from lhn.header import F, gamma, SparkConf, spark, FloatType, Broadcast
from lhn.header import get_logger
# Get the logger configured in __init__.py
logger = get_logger(__name__)
 


#from pyspark.sql import functions as F
def calculate_percentile_pyspark(observed_A, expected_total, alpha_prior=1, beta_prior=1):
    alpha = observed_A + alpha_prior
    beta = expected_total + beta_prior
    rv = gamma(alpha, scale=1/beta)
    result = rv.sf(observed_A)
    return float(result)

calculate_percentile_pyspark_udf = F.udf(calculate_percentile_pyspark, FloatType())




def five_number_summary(df, index, groupby, value):
    """Calculate a five number summary for a given column in a DataFrame, grouped by another column.

    Args:
        df (Spark DataFrame): A Spark DataFrame
        index (list): The index columns
        groupby (list): The column to group by
        value (str): The column to calculate the five number summary for
    """
    # Convert to Pandas DataFrame
    pandas_df = df.select([*index, *groupby, value]).distinct().toPandas()
    for col in groupby:
        pandas_df[col] = pandas_df[col].astype(str) # Fill in missing values
    result = pandas_df.groupby(groupby).agg(
        **{f'Unique_{col}': (col, 'nunique') for col in index},
        average=(value, 'mean'),
        min=(value, 'min'),
        max=(value, 'max'),
        Q1=(value, lambda x: x.quantile(0.25)),
        median=(value, 'median'),
        Q3=(value, lambda x: x.quantile(0.75))
    )
    return result

# Sample usage with dummy data
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[1]").appName('SparkApp').getOrCreate()
# data = spark.createDataFrame(...)
# result = calculate_chi_squared(data, indexFields=['id'], groupFields=['group'], outcomeFields=['outcome'])
# result.show()


# Sample usage
# result = calculate_chi_squared(data, indexFields=['id'], groupFields=['group'], outcomeFields=['outcome'], alpha_prior=10, beta_prior=10)
# result.show()

def oe_table(df, alpha_prior=1, beta_prior=1):
    """
    not a worked function, just saving code
    """
    oe_table_pd = oe_table.toPandas()
    oe_table_pd['Percentile05'] = (oe_table_pd
                                .apply(lambda row: gamma_percentile(row['observed_num'] + alpha_prior, row['observed_den'] + beta_prior), axis=1)
    )
    oe_table_spark = SparkConf.createDataFrame(oe_table_pd)
    #oe_table = oe_table.withColumn('Percentile05',
    #                               gamma_percentile_udf(F.col('observed_num') + F.lit(alpha_prior),
    #                                                    F.col('observed_den') + F.lit(beta_prior)))
    #return oe_table.select(groupFields + outcomeFields + ['observed_over_expected', 'Percentile05'])
    return oe_table

def calculate_percentile(row, observed, expected, alpha=1, beta=1):
    """
    Compare the adjusted O/E to the gamma distribution given by alpha , beta
    """
    
    alpha_Prior_num = row[alpha] if isinstance(alpha, str) else alpha
    beta_Prior_num = row[beta] if isinstance(beta, str) else beta
    
    alpha_Posterior_num = row[observed] + alpha_Prior_num
    beta_Posterior_num = row[expected] + beta_Prior_num
    observed_A = row[observed]
    oe = alpha_Posterior_num/beta_Posterior_num
    rv = gamma(alpha_Prior_num, scale=1/beta_Prior_num)
    result = rv.sf(oe)
    return result

# calculate_percentile_udf = F.udf(calculate_percentile)


def gamma_percentile(a, b, p = 0.05):
    
    rv = gamma(a, scale=1/b)
    return float(rv.ppf(p))

    
gamma_percentile_udf = F.udf(gamma_percentile)

def calculate_chi_squared(data, indexFields, groupFields, outcomeFields, alpha_prior=1, beta_prior=1):
    
    """
    Purpose       : Calculate Observed/Expected with Expected calulated similar to as in a contigency table
    data          : A Pandas Data Table
    indexFields   : An observational unit, such as `personid`
    groupFields   : This identifies the `treatments` or in ML language the feature groups
    outcomeFields : This is the `target` or `outcome or tag
    alpha_prior   : This is a discount factor for the number units per observed counts.
    beta_prior    : This is a discount factor for the number of units per expected counts.
    """
    # This is how the FDA does it.
    #    # 2x2 Contingency Table for Drug Exposure and Event Occurrence
    #    
    # |                     | Event Present (E+) | Event Absent (E-) |
    # |---------------------|---------------------|-------------------|
    # | Exposed to Drug (D+)| A                       | B                 |
    # | Not Exposed (D-)    | C                   | D                 |
    #
    # Explanation:
    # A = Number of exposed individuals who experienced the event
    # B = Number of exposed individuals who did not experience the event
    # C = Number of unexposed individuals who experienced the event
    # D = Number of unexposed individuals who did not experience the event
    #
    # Calculating Expected and Observed Counts:
    # Observed Count for those exposed to the drug = A
    # Expected Count for those exposed to the drug is calculated based on the proportion of individuals
    # with the event in the entire population (both exposed and unexposed) and the total number exposed to the drug.
    # Expected Count Formula: (A + B) * (A + C) / (A + B + C + D)
    # This formula estimates the number of events expected in the exposed group if the event occurred
    # at the same rate as it does in the entire population.


    # Count the number of distinct index fields for each group Field and outcome combination. (A)
    observed_A   = data.groupBy(groupFields + outcomeFields).agg(F.countDistinct(*indexFields).alias('observed_A'))
    # Count the number of distinct index fields for each group. (A + B)
    group_A_B    = data.groupBy(groupFields).agg(F.countDistinct(*indexFields).alias('group_A_B'))
    # Calculate the proportion of times each outcome occurs in the data. (A + C)  
    event_A_C    = data.groupBy(outcomeFields).agg(F.countDistinct(*indexFields).alias('event_A_C'))
    # Calculate the total number of observed counts in the data. (A + B + C + D)
    expected_denominator = data.agg(F.countDistinct(*indexFields)).collect()[0][0]  # A+B+C+D
    # For each group calculate the ratio of observed counts by outcome to the total number of observed counts in each group.
    # This is the probability of the outcome given the group.
    oe_table = (
        observed_A.join(group_A_B, on=groupFields, how='left')
        .join(event_A_C, on=outcomeFields, how='left')
        # Observed Ratio = A / (A + B)
        .withColumn('observed_ratio', F.col('observed_A') / F.col('group_A_B'))
        .withColumn('total_A_B_C_D', F.lit(expected_denominator))
        # Prevalence Ratio = (A + C)/ (A + B + C + D)
        .withColumn('prevalence_ratio', F.col('event_A_C') / F.col('total_A_B_C_D'))
        # (A + B) * (A + C) / (A + B + C + D)
        .withColumn('expected_total', F.col('group_A_B') * F.col('event_A_C') / F.col('total_A_B_C_D'))
        # Observed/Expected
        .withColumn('observed_over_expected', F.col('observed_A') / F.col('expected_total'))
        .withColumn('alpha_prior', F.lit(alpha_prior))
        .withColumn('beta_prior', F.lit(beta_prior))
        .withColumn('alpha_post', F.col('observed_A') + F.col('alpha_prior'))
        .withColumn('beta_post',  F.col('expected_total') + F.col('beta_prior')  )
        .withColumn('observed_over_expected_Adj', F.col('alpha_post') / F.col('beta_post'))
        )
    # Fix this to work with Pandas
    
    oe_table_pd = oe_table.toPandas()
    oe_table_pd['Percentile05'] = oe_table_pd.apply(lambda row: gamma_percentile(row['alpha_post'], row['beta_post'], 0.05), axis=1)
    oe_table_pd['Percentile95'] = oe_table_pd.apply(lambda row: gamma_percentile(row['alpha_post'], row['beta_post'], 0.95), axis=1)
    oe_table_pd['observed_pvalue'] = oe_table_pd.apply(
        lambda row: calculate_percentile(row, observed = 'observed_A', expected = 'expected_total', alpha = 'alpha_prior', beta = 'beta_prior'), axis=1)
    
    oe_table_pd['observed_pvalue_pre'] = oe_table_pd.apply(
        lambda row: calculate_percentile(row, observed = 'observed_A', expected = 'expected_total', alpha = 'alpha_post', beta = 'beta_post'), axis=1)
    
    # Sort by 'Percentile05' in descending order
    oe_table_pd = oe_table_pd.sort_values(by='Percentile05', ascending=False)
    
    # Add new column 'NotContainOne'
    oe_table_pd['Signal'] = ~oe_table_pd.apply(lambda row: row['Percentile05'] <= 1 <= row['Percentile95'], axis=1)
    oe_table_pd['SignalPositive'] = oe_table_pd.apply(lambda row: row['Percentile05'] > 1, axis=1)
    
    return oe_table_pd


    




