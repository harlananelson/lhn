from lhn.header import F, Window
###########################################################################
##### Matched Cohort
##########################################################################
from lhn.header import get_logger

logger = get_logger(__name__)

def call_match_controls(cohort_in, demo_in):
    """
    Assign controls to cases.

    Parameters
    ----------
    cohort_in : DataFrame
        A DataFrame containing a list of personid and matching fields. These need matching controls.
    demo_in : DataFrame
        A DataFrame of all eligible personids including cases and potential controls.

    Example
    -------
    >>> cohort = e.scdpatient.df
    >>> demo = m.demo
    >>> matched_controls = call_match_controls(cohort_in=cohort, demo_in=demo)

    Notes
    -----
    The function's implementation is expected to carry out the matching process and return a DataFrame
    with the matched controls. The example provided assumes the existence of an environment with
    'e.scdpatient.df' and 'm.demo' variables set.

    Returns
    -------
    DataFrame
        The resulting DataFrame after matching controls to cases.
    """
    # Function implementation goes here
    
    # These asignment could be put into the funtion header with these default values.
    index_match    = ['gender', 'age', 'race', 'ethnicity', 'tenant']   # Control Patients are matched to Case Patients
    index_distance = ['encounters', 'followtime']                     # Used in addition to index_match to minimize "distance" to case Patients
    index_person   = ['personid']
    demoFieldsAll  = index_person + index_match + index_distance
    demoVector      = index_person + index_match + ["encounters_standardized", "followtime_standardized"]
    demoFields     = index_person + index_match + ['distance']
    joinedFields   = index_person + ['personidCase'] + index_match + ['distance']
    fieldsCase     = [ 'personidCase', *index_match, "encounters_standardizedCase", "followtime_standardizedCase"] 
    fieldsControl  = [ *index_person,  *index_match, *index_distance] 
    fieldsControl0 = [ *index_person, *index_match,  "encounters_standardized","followtime_standardized"] 
    ICD_levels     = ['SCD']
    Lab_levels     = ['SCD_SCA', 'SCD_SC', 'SCD_SCA_Likely', 'SCD_Indeterminate', 'SCD_Sbetap_Likely','SCD_SE', 'SCD_SD']
    hispanic_list = ["Hispanic or Latino", "Dominican", "Central American", "Puerto Rican", "South American", "Mexican", "Latin American", "Salvadoran", "Honduran", "Spaniard", "Spanish Basque", "Ecuadorian", "Cuban", "Central American Indian", "Andalusian", "Chilean", "Guatemalan"]
    non_hispanic_list = ["Not Hispanic or Latino", "Unknown", "African American", "Race reported", "Patient Refused or Declined", "English", "Other", "Multiple", "Caribbean Island (NMO)", "American Indian or Alaska native"]

    #Modify demo_in to have engineered matching feature.
    demo = (
        demo_in
        .withColumn("grouped_race",
                    F.when(F.col("race") == "Black or African American", "Black")
                    .when(F.col("race") == "White or Caucasian", "White")
                    .otherwise("Other")
                    )
        .drop('race')
        .withColumnRenamed('grouped_race', 'race')
        .withColumn("grouped_ethnicity",
                    F.when(F.col("ethnicity").isin(hispanic_list), "Hispanic")
                    .when(F.col("ethnicity").isin(non_hispanic_list), "Non-Hispanic")
                    .otherwise("Other")
                    )
        .drop('ethnicity')
        .withColumnRenamed('grouped_ethnicity', 'ethnicity')
        .withColumn("age_group",
                    F.when((F.col("age") >= 0) & (F.col("age") <= 2), "Infants and Toddlers")
                    .when((F.col("age") >= 3) & (F.col("age") <= 5), "Preschoolers")
                    .when((F.col("age") >= 6) & (F.col("age") <= 12), "School age")
                    .when((F.col("age") >= 13) & (F.col("age") <= 19), "Teenagers")
                    .when((F.col("age") >= 20) & (F.col("age") <= 29), "Young Adults")
                    .when((F.col("age") >= 30) & (F.col("age") <= 39), "Adults")
                    .otherwise("Other")  # If there are older ages not covered in the given data
                    )
        .drop('age')
        .withColumnRenamed('age_group', 'age')
    )

    # 1. Calculate mean and stddev for encounters and followtime: Used as a distance meausure of case control matching
    stats = (demo
            .select(demoFieldsAll)
            .agg(F.mean( "encounters").alias("mean_encounters"),
                F.stddev("encounters").alias("stddev_encounters"),
                F.mean(  "followtime").alias("mean_followtime"),
                F.stddev("followtime").alias("stddev_followtime"))
            )

    # 2. Join the stats back to the original DataFrame (this will result in the entire DataFrame having repeated mean and stddev columns)
    demo_with_stats = demo.crossJoin(F.broadcast(stats))

    # 3. Compute the standardized fields
    demo_with_vector = (demo_with_stats
                                .withColumn("encounters_standardized", 
                                            (F.col("encounters") - F.col("mean_encounters")) / F.col("stddev_encounters"))
                                .withColumn("followtime_standardized", 
                                            (F.col("followtime") - F.col("mean_followtime")) / F.col("stddev_followtime"))
                                .drop("mean_encounters", "stddev_encounters", "mean_followtime", "stddev_followtime")  # Drop the mean and stddev columns
                                .select(demoVector)
                            )

    # A list of all patients who will not be used as controls
    # Add the engineered matching fields from demo
    case_superset = (
        cohort_in.select([*index_person, 'IcdPheno', 'PersonPhenotype'])
        .join(demo_with_vector, on = ['personid'], how = 'inner')
        .cache()
        )

    # The list of case people
    # Modify fields so they can be identified as originating with the case people
    print(f'The columns of case_superset are {case_superset.schema}')
    print(f'fieldsCase: {fieldsCase}')
    case = (
        case_superset
        .filter(F.col('IcdPheno').isin(ICD_levels) | F.col('PersonPhenotype').isin(Lab_levels))
        .withColumnRenamed( 'personid', 'personidCase')     # Track the ID of the case then the control will be personid
        .withColumnRenamed( "encounters_standardized", "encounters_standardizedCase")
        .withColumnRenamed( "followtime_standardized", "followtime_standardizedCase")
        .select(fieldsCase)
        .distinct()
        #.limit(10)
        .cache()
    )


    # People that can be used as controls
    controlpool_raw = (
        demo_with_vector
        .join(case_superset, on = index_person, how = 'leftanti')  # take people not wanted as controls
        .filter(
            F.col("encounters_standardized").isNotNull() & 
            F.col("followtime_standardized").isNotNull()
        )  # Filtering out rows with None values
        .select(fieldsControl0)
    )
    
    # Initialize variables to track cases and controls
    remaining_cases = case
    remaining_controls = controlpool_raw
    all_matched_controls = None  # To store all controls matched across iterations

    # Iteratively match controls
    while True:
        # Perform the match for the remaining cases and controls
        new_matched_controls = match_controls_to_cases(remaining_cases, remaining_controls)
        
        # Combine with previously matched controls
        if all_matched_controls:
            all_matched_controls = all_matched_controls.union(new_matched_controls)
        else:
            all_matched_controls = new_matched_controls

        # Identify cases that did not receive four controls
        case_count = all_matched_controls.groupBy('personidCase').count()
        insufficient_controls_cases = case_count.filter(F.col('count') < 4)

        # Identify remaining unused controls
        used_controls = all_matched_controls.select('personid').distinct()
        remaining_controls = remaining_controls.join(used_controls, on='personid', how='left_anti')
        
        # If no more cases or controls left to match, exit loop
        if insufficient_controls_cases.count() == 0 or remaining_controls.count() == 0:
            break
        
        # Otherwise, update the remaining cases and continue the loop
        remaining_cases = remaining_cases.join(insufficient_controls_cases.select('personidCase'), on='personidCase', how='inner')

    # At this point, 'all_matched_controls' should have as many matched controls for each case as possible
 

    
def match_controls_to_cases(case, controlpool_raw, 
                            index_match_current    = ['gender', 'age', 'race', 'ethnicity', 'tenant']   # Control Patients are matched to Case Patients
                            ):
    # return: firstFourControls: The latest assignment
    # input: 
    #  - e.scdpatient.df: A list of 'personid' with fields 'IcdPheno', 'PersonPhenotype'  This is the cohort
    #  - m.demo: A table of the universe of people with fiels 'pesonid', gender', 'age', 'race','tenant', 'zip_code', 'encounters', 'followtime'

    index_match = ['gender', 'age', 'race', 'ethnicity', 'tenant']
    index_distance = ['encounters', 'followtime']                     # Used in addition to index_match to minimize "distance" to case Patients
    index_person   = ['personid']
    demoFieldsAll  = index_person + index_match + index_distance
    demoVector      = index_person + index_match + ["encounters_standardized", "followtime_standardized"]
    demoFields     = index_person + index_match + ['distance']
    joinedFields   = index_person + ['personidCase'] + index_match + ['distance']
    fieldsCase     = [ 'personidCase', *index_match, "encounters_standardizedCase", "followtime_standardizedCase"] 
    fieldsControl  = [ *index_person,  *index_match, *index_distance] 
    fieldsControl0 = [ *index_person, *index_match,  "encounters_standardized","followtime_standardized"] 

    # Calculate distance using Euclidean distance formula for two dimensions
    controlJoinedToCase = (
        controlpool_raw.select(index_person + index_match_current + ["encounters_standardized", "followtime_standardized"])
        .join(case, on = index_match_current, how = 'inner')
        .withColumn('distance', 
                    F.sqrt(
                        F.pow(F.coalesce(F.col("encounters_standardizedCase"), F.lit(0)) - F.coalesce(F.col("encounters_standardized"), F.lit(0)), 2) + 
                        F.pow(F.coalesce(F.col("followtime_standardizedCase"), F.lit(0)) - F.coalesce(F.col("followtime_standardized"), F.lit(0)), 2) +
                    (F.rand() * 0.0001))
                    ) # Add a small random number to ensure unique distances
        .select(joinedFields)
                    )
    
    # input: controlJoinedToCase.  All cases matched to controls by index_match, with a measure distance giving the distance between the case and control
    # 1. Select what case each control is matched to by selecting the case closest to the control using distance.  
    # 2. Each case should now be associated with many controls, Select 4 controls by selecting the controls closest to each case using distance.
    # Note that becasue cases and controls were joined in controlJoinedToCase by index_match, these steps can be done within the levels of index_match

    # Define a window specification
    windowSpecControl = Window.partitionBy([*index_match_current, 'personid']) \
                    .orderBy(F.col("distance"))

    # Add a row number over the window
    controlWithRowNumber = controlJoinedToCase.withColumn("row_number", F.row_number().over(windowSpecControl))

    # Filter to only take the first row of each partition
    firstControl = controlWithRowNumber.filter(F.col("row_number") == 1).drop("row_number")

    # Define a window specification
    windowSpecCase = Window.partitionBy([*index_match_current, 'personidCase']) \
                    .orderBy(F.col("distance"))

    # Add a row number over the window
    firstCaseWithRowNumber = firstControl.withColumn("row_number", F.row_number().over(windowSpecCase))

    # Filter to only take the first 4 rows of each partition
    firstFourControls = firstCaseWithRowNumber.filter(F.col("row_number") <= 4).drop("row_number").cache()
    print(firstFourControls.limit(1).toPandas())
    
    return firstFourControls  # This should be a DataFrame containing the controls closest to each case
