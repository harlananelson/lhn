from lhn.data_display import print_pd
from lhn.query import extract_fields_flat_top
from lhn.data_summary import attrition
from lhn.header import get_logger, F

from lhn.spark_utils import to_parquet as to_parquet_func


# Get the logger configured in __init__.py
logger = get_logger(__name__)

class SharedMethodsMixin:
    def attrition(self):
        datefield = getattr(self, 'datefield', None)
        logger.info(f"attrition({self.df}, {self.name}, personid, {self.label}, {datefield})")
        attrition(df=self.df, table_name=self.name, person_id='personid', description=self.label, 
                  date_field=datefield)

    def tabulate(self, by, index=['personid'], debug=None, obs=300, toPandas=True, countfield='subjects', 
                 sortfield=None, display=True, returnDataTable=None, ascending=False):
        """
        A method wrapper around the function extract_fields_flat_top.
        This method aggregates the data in the DataFrame based on the specified fields and optionally displays
        the result or returns it as a Spark DataFrame or a Pandas DataFrame.

        Args:
            by (list or str): The field(s) to group the data by.
            index (list, optional): The field(s) to use as the index for the aggregated data. Default is ['personid'].
            debug (bool, optional): If True, prints debug information. Default is None, which uses the self.debug value.
            obs (int, optional): The maximum number of rows to display. Default is 300.
            toPandas (bool, optional): If True, converts the result to a Pandas DataFrame before returning. Default is True.
            countfield (str, optional): The field to use for counting. Default is 'subjects'.
            sortfield (list or str, optional): The field(s) to sort the result by. Default is None, which uses the countfield.
            display (bool, optional): If True, displays the result using print_pd(). Default is True.
            returnDataTable (bool, optional): If True, returns the result DataFrame. Default is None.
            ascending (bool, optional): Sort order for the result. Default is False (descending).

        Returns:
            If returnDataTable is True, returns a Pandas DataFrame (if toPandas=True) or Spark DataFrame (if toPandas=False).
            Otherwise, returns None (result is displayed).
        """
        toPandas = toPandas if toPandas is not None else True
        sortfield = sortfield if sortfield is not None else countfield
        debug = debug if debug is not None else getattr(self, 'debug', False)
        
        if display is not None and display and returnDataTable is None:
            returnDataTable = False

        if not all(item in self.df.columns for item in index):
            if hasattr(self, 'index') and all(item in self.df.columns for item in getattr(self, 'index')):
                index = getattr(self, 'index')
            else:
                other_index_fields = ['personid', 'person_id', 'provider_id']
                available_index_fields = list(set(other_index_fields) & set(self.df.columns))
                if not available_index_fields:
                    logger.warning("No suitable index fields found in the table. Using the 'by' fields as index fields.")
                    return None
                index = available_index_fields

        logger.info(f"Using index fields: {index}, display:{display}, toPandas:{toPandas}, ascending {ascending}")

        result = extract_fields_flat_top(self.df, by, index=index, obs=obs, toPandas=False, debug=debug, countfield=countfield)
        if toPandas:
            result = result.toPandas()
            if sortfield is not None:
                result = result.sort_values(by=sortfield, ascending=ascending)
            if display is not None and display:
                print_pd(result, label='', obs=obs)
        else:
            if sortfield is not None:
                result = result.orderBy(sortfield, ascending=ascending)
        logger.info(f"returnDataTable: {returnDataTable}")
        if returnDataTable:
            return result
        else:
            return None

    def print_pd(self, obs=300, label='', sortfield='Subjects', sort_order='desc'):
        print_pd(self.df, obs=obs, label=label, sortfield=sortfield, sort_order=sort_order)

    def to_csv(self):
        """
        Write the Spark DataFrame to a CSV file at the location specified by self.csv.
        """
        if not hasattr(self, 'csv'):
            logger.error("No csv attribute defined. Cannot write to CSV.")
            return
        if self.df.rdd.isEmpty():
            logger.warning("DataFrame is empty. Skipping CSV write operation.")
        else:
            logger.info(f"Writing to CSV file at {self.csv}")
            self.df.toPandas().to_csv(self.csv, index=False)
            
            

    def to_parquet(self, path: str = None, partitionBy: str = None, mode: str = "overwrite", fallback_dir: str = "/tmp", retries: int = 3, retry_delay: float = 2.0) -> None:
        """
        Write the Spark DataFrame to a Parquet file at the specified path or self.parquet.

        Calls the standalone to_parquet function in spark_utils.py.

        Args:
            path (str, optional): The file path (e.g., 'hdfs:///user/hnelson3/...'). Defaults to self.parquet.
            partitionBy (str, optional): The column to partition the data by.
            mode (str): The write mode ('overwrite', 'append', etc.). Defaults to 'overwrite'.
            fallback_dir (str): Fallback HDFS directory if the specified path is not writable. Defaults to '/tmp'.
            retries (int): Number of retries for HDFS commands. Defaults to 3.
            retry_delay (float): Delay between retries in seconds. Defaults to 2.0.

        Raises:
            ValueError: If no valid path is provided.
            RuntimeError: If the write operation fails.
        """
        target_path = path if path is not None else getattr(self, 'parquet', None)
        if not target_path:
            # Default to HDFS user directory if self.parquet is unset
            target_path = f"hdfs:///user/hnelson3/SickleCell/{self.label.replace(' ', '_')}.parquet"
            logger.info(f"No parquet attribute defined. Using default HDFS path: {target_path}")

        to_parquet_func(self.df, target_path, partitionBy=partitionBy, mode=mode, fallback_dir=fallback_dir, retries=retries, retry_delay=retry_delay)