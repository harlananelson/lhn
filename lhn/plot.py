
from lhn.header import F, pn, datetime, TimestampType, DataFrame, StringType, pd, DateType, IntegerType, np, Window
import plotly.express as px

from .header import get_logger

logger = get_logger(__name__)

def count(df, fieldName):
    result = df.groupby(F.col(fieldName)).count().withColumn('countln', F.log(F.col('count'))).toPandas()
    return(result)


def plot_counts(df, x, y, xlab='', ylab='', title = '', vlines=[], hlines=[], grouping=None):
    """
    Plots a scatter plot of counts with optional vertical and horizontal lines.

    Parameters:
    df (pandas.DataFrame): The dataframe containing the data to plot.
    x (str): The column name for the x-axis values.
    y (str): The column name for the y-axis values.
    xlab (str, optional): The label for the x-axis. Defaults to ''.
    ylab (str, optional): The label for the y-axis. Defaults to ''.
    title (str, optional): The title of the plot. Defaults to ''.
    vlines (list, optional): A list of x-axis values where vertical dashed lines should be drawn. Defaults to [].
    hlines (list, optional): A list of y-axis values where horizontal dashed lines should be drawn. Defaults to [].
    grouping (str, optional): The column name for the grouping variable. If provided, points will be colored by this variable. Defaults to None.

    Returns:
    plotnine.ggplot: A plotnine scatter plot.
    """
    
    plot_arguments = {'x': x, 'y': y}
    if grouping is not None:
        df[grouping] = pd.Categorical(df[grouping])
        plot_arguments['color'] = grouping
    plot_result = (pn.ggplot(df, pn.aes(**plot_arguments))
    + pn.geom_point()
    + pn.scale_x_log10()
    + pn.scale_y_log10()
    + pn.labs(title = title, y = ylab, x = xlab)
    )
    for line in vlines:
        plot_result += pn.geom_vline(xintercept=line, linetype="dashed", color="red")
    for line in hlines:
        plot_result += pn.geom_hline(yintercept=line, linetype="dashed", color="blue")
    return(plot_result)

#from pyspark.sql.types import TimestampType, StringType
#from plotnine import ggplot, aes, geom_point, scale_y_log10, labs, theme, element_text, scale_x_date, geom_vline, annotate


def plotByTime(df, datefield='date', xlab='', ylab='', title='', date_low='2019-01-01',
               surging_dates='COVID-19', count=None, date_break="2 year", date_high=None,
               annotate_offset=10, useLog = True, grouping = None, plot_proportions=False, time_group='day'):
    """
    Plots a scatter plot of counts over time with optional vertical lines.

    Parameters:
    df (pandas.DataFrame): The dataframe containing the data to plot.
    datefield (str, optional): The column name for the date values. Defaults to 'date'.
    xlab (str, optional): The label for the x-axis. Defaults to ''.
    ylab (str, optional): The label for the y-axis. Defaults to ''.
    title (str, optional): The title of the plot. Defaults to ''.
    date_low (str, optional): The lower limit for the date range. Defaults to '2019-01-01'.
    surging_dates (str or list, optional): A list of dates where vertical dashed lines should be drawn. Defaults to 'COVID-19'.
    count (str, optional): The column name for the count values. If None, the function will count the number of records for each date. Defaults to None.
    date_break (str, optional): The interval for the date breaks on the x-axis. Defaults to '2 year'.
    date_high (str, optional): The upper limit for the date range. If None, the function will use the current date. Defaults to None.
    annotate_offset (int, optional): The y-axis value for the annotations. Defaults to 10.
    useLog (bool, optional): Whether to use a log scale for the y-axis. Defaults to True.
    grouping (str, optional): The column name for the grouping variable. If provided, points will be colored by this variable. Defaults to None.
    plot_proportions (bool, optional): Whether to plot proportions instead of counts. Defaults to False.
    time_group (str, optional): The time unit for grouping the data. Allowed values are 'day', 'week', 'month', 'year'. Defaults to 'day'.

    Returns:
    plotnine.ggplot: A plotnine scatter plot.
    """
    # function body...
    
    # Check if time_group is one of the allowed values
    if time_group not in ['day', 'week', 'month', 'year']:
        print(f"Error: Invalid time_group '{time_group}'. Allowed values are 'day', 'week', 'month', 'year'.")
        return
    
    # Map time_group to the corresponding values used by date_trunc and to_period
    time_group_mapping = {'day': 'D', 'week': 'W', 'month': 'M', 'year': 'Y'}
    
    countField         = count if count else 'count'
    datefieldIsNumeric = False
    plot_arguments     = {'x': datefield, 'y': countField}
    
    if surging_dates == 'COVID-19':
        surging_dates = ['2020-03-01', '2020-07-01', '2020-11-01', '2021-01-01', '2021-12-01', '2022-08-01']
    elif surging_dates == 'COVID-19 Vaccine':
        surging_dates = ['2020-12-14', '2021-04-19', '2021-08-13', '2021-09-20', '2021-11-19', '2022-03-29', '2022-09-02']
    else:
        log(f"surging_dates = {surging_dates}")
        surging_dates = surging_dates
        
    if date_high is None:
        date_high = datetime.now().date()
        
    if grouping is not None:    
        plot_arguments['color'] = grouping
        groupFields = [F.col(datefield), F.col(grouping)]
    else:
        groupFields = [F.col(datefield)]
        
    
    if isinstance(df, DataFrame):
        dataType = 'Spark'
    elif isinstance(df, pd.DataFrame):
        dataType = 'Pandas'
    else:
        print("Don't know the data type")
        # might need to exit.
        
    if dataType == 'Spark':
        
        # Check if the datefield is a number and convert it to a Date if it is
        # Check if the datefield is a number and convert it to a Date if it is
        if isinstance(df.schema[datefield].dataType, IntegerType):
            print("datefield is an integer, skipping conversion to date")
            datefieldIsNumeric = True
        else:
            print("converting to date")
            # Try to convert the datefield to a date
            df = df.withColumn('datefield_temp', F.to_date(F.col(datefield)))

            # Create a new column that is True when the conversion was successful and False otherwise
            df = df.withColumn('is_valid_date', F.when(F.col('datefield_temp').isNull(), False).otherwise(True))

            # Filter out the records where the conversion failed
            df = df.filter(F.col('is_valid_date'))

            # Drop the is_valid_date column
            df = df.drop('is_valid_date')
            df = df.drop(datefield)

            # Rename the temporary datefield column to the original column name
            df = df.withColumnRenamed('datefield_temp', datefield)
            
            # Group by time_group
            df = df.withColumn(datefield, F.date_trunc(time_group, F.col(datefield)))
            

        if not count or (countField not in df.columns):
            df = df.groupBy(groupFields).count().withColumnRenamed('count', countField)
            if plot_proportions and grouping is not None:
                window = Window.partitionBy(grouping)
                df = df.withColumn('total', F.sum(F.col(countField)).over(window))
                df = df.withColumn(countField, F.col(countField) / F.col('total'))
            df = df.sort(F.col(countField).desc())
            
        # Filter by date range
        df = df.filter((F.col(datefield) >= F.lit(date_low)) & 
                    (F.col(datefield) <= F.lit(date_high))).sort(F.col(datefield))
        df = df.toPandas()
        
    elif dataType == 'Pandas':
        
        # Check if the datefield is a datetime and convert it to a date if it is
        if df[datefield].dtype in ['datetime64[ns]']:
            df[datefield] = df[datefield].dt.date
            df[datefield] = df[datefield].dt.to_period(time_group_mapping[time_group]).dt.to_timestamp()

        elif df[datefield].dtype in ['int64', 'int32']:
            print("datefield is an integer, skipping conversion to date")
            datefieldIsNumeric = True

        if not count or (countField not in df.columns):
            df = df.groupby(groupFields).size().reset_index(name=countField).sort_values(by=countField, ascending=False)
            if plot_proportions and grouping is not None:
                total = df.groupby(grouping)[countField].transform('sum')
                df[countField] = df[countField] / total

        # Filter by date range
        if datefieldIsNumeric:
            df = df[(df[datefield] >= date_low) & (df[datefield] <= date_high)]
        else:
            df = df[(df[datefield] >= pd.to_datetime(date_low).date()) & (df[datefield] <= pd.to_datetime(date_high).date())]
            
    if grouping is not None:
        df[grouping] = df[grouping].astype(str) 
     # Convert the grouping column to string to ensure discrete colors
     
    if plot_proportions:
        ylab = 'Proportion'
    
    if datefieldIsNumeric:
        print("datefield is numeric")
        result = (pn.ggplot(df, pn.aes(**plot_arguments))
            + pn.geom_point()
            + pn.labs(title=title, y=ylab, x=xlab)
            + pn.theme(axis_text_x=pn.element_text(angle=90, hjust=1))
            + pn.scale_x_continuous()  # Modify this line
        )
    else:
        result = (pn.ggplot(df, pn.aes(**plot_arguments))
            + pn.geom_point()
            + pn.labs(title=title, y=ylab, x=xlab)
            + pn.theme(axis_text_x=pn.element_text(angle=90, hjust=1))
            + pn.scale_x_date(date_breaks=date_break, date_labels="%b %Y")
        )
        
    if useLog:
        print("log scale")
        result += pn.scale_y_log10()

    if surging_dates is not None:
        print("surging dates")
        if isinstance(df[datefield].dtype, np.number):
            # Handle surging_dates and annotate differently when datefield is a number
            result += pn.geom_vline(xintercept=surging_dates, linetype="dashed", color="red")
            result += pn.annotate("text", x=surging_dates, y=annotate_offset, label=surging_dates, 
                   angle = 90, size = 8)
    if grouping is not None:
        pass
        #result += scale_color_manual(values=df[grouping].astype('category').cat.codes)
    
    return result


def plotByTime(df, datefield='date', xlab='', ylab='', title='', date_low='2019-01-01',
               surging_dates='COVID-19', count=None, date_break="2 year", date_high=None,
               annotate_offset=10, useLog = True, grouping = None, plot_proportions=False, 
               time_group='day', plot_lib='plotnine'):
    """
    Plots a scatter plot of counts over time with optional vertical lines.

    Parameters:
    df (pandas.DataFrame): The dataframe containing the data to plot.
    datefield (str, optional): The column name for the date values. Defaults to 'date'.
    xlab (str, optional): The label for the x-axis. Defaults to ''.
    ylab (str, optional): The label for the y-axis. Defaults to ''.
    title (str, optional): The title of the plot. Defaults to ''.
    date_low (str, optional): The lower limit for the date range. Defaults to '2019-01-01'.
    surging_dates (str or list, optional): A list of dates where vertical dashed lines should be drawn. Defaults to 'COVID-19'.
    count (str, optional): The column name for the count values. If None, the function will count the number of records for each date. Defaults to None.
    date_break (str, optional): The interval for the date breaks on the x-axis. Defaults to '2 year'.
    date_high (str, optional): The upper limit for the date range. If None, the function will use the current date. Defaults to None.
    annotate_offset (int, optional): The y-axis value for the annotations. Defaults to 10.
    useLog (bool, optional): Whether to use a log scale for the y-axis. Defaults to True.
    grouping (str, optional): The column name for the grouping variable. If provided, points will be colored by this variable. Defaults to None.
    plot_proportions (bool, optional): Whether to plot proportions instead of counts. Defaults to False.
    time_group (str, optional): The time unit for grouping the data. Allowed values are 'day', 'week', 'month', 'year'. Defaults to 'day'.

    Returns:
    plotnine.ggplot: A plotnine scatter plot.
    """
    # function body...
    
    # Check if time_group is one of the allowed values
    if time_group not in ['day', 'week', 'month', 'year']:
        print(f"Error: Invalid time_group '{time_group}'. Allowed values are 'day', 'week', 'month', 'year'.")
        return
    
    # Map time_group to the corresponding values used by date_trunc and to_period
    time_group_mapping = {'day': 'D', 'week': 'W', 'month': 'M', 'year': 'Y'}
    
    countField         = count if count else 'count'
    datefieldIsNumeric = False
    plot_arguments     = {'x': datefield, 'y': countField}
    
    if surging_dates == 'COVID-19':
        surging_dates = ['2020-03-01', '2020-07-01', '2020-11-01', '2021-01-01', '2021-12-01', '2022-08-01']
    elif surging_dates == 'COVID-19 Vaccine':
        surging_dates = ['2020-12-14', '2021-04-19', '2021-08-13', '2021-09-20', '2021-11-19', '2022-03-29', '2022-09-02']
    else:
        print(f"surging_dates = {surging_dates}")
        surging_dates = surging_dates
        
    if date_high is None:
        date_high = datetime.now().date()
        
    if grouping is not None:    
        plot_arguments['color'] = grouping
        groupFields = [F.col(datefield), F.col(grouping)]
    else:
        groupFields = [F.col(datefield)]
        
    
    if isinstance(df, DataFrame):
        dataType = 'Spark'
    elif isinstance(df, pd.DataFrame):
        dataType = 'Pandas'
    else:
        print("Don't know the data type")
        # might need to exit.
        
    if dataType == 'Spark':
        
        # Check if the datefield is a number and convert it to a Date if it is
        # Check if the datefield is a number and convert it to a Date if it is
        if isinstance(df.schema[datefield].dataType, IntegerType):
            print("datefield is an integer, skipping conversion to date")
            datefieldIsNumeric = True
        else:
            print("converting to date")
            # Try to convert the datefield to a date
            df = df.withColumn('datefield_temp', F.to_date(F.col(datefield)))

            # Create a new column that is True when the conversion was successful and False otherwise
            df = df.withColumn('is_valid_date', F.when(F.col('datefield_temp').isNull(), False).otherwise(True))

            # Filter out the records where the conversion failed
            df = df.filter(F.col('is_valid_date'))

            # Drop the is_valid_date column
            df = df.drop('is_valid_date')
            df = df.drop(datefield)

            # Rename the temporary datefield column to the original column name
            df = df.withColumnRenamed('datefield_temp', datefield)
            
            # Group by time_group
            df = df.withColumn(datefield, F.date_trunc(time_group, F.col(datefield)))
            

        if not count or (countField not in df.columns):
            df = df.groupBy(groupFields).count().withColumnRenamed('count', countField)
            if plot_proportions and grouping is not None:
                window = Window.partitionBy(grouping)
                df = df.withColumn('total', F.sum(F.col(countField)).over(window))
                df = df.withColumn(countField, F.col(countField) / F.col('total'))
            df = df.sort(F.col(countField).desc())
            
        # Filter by date range
        df = df.filter((F.col(datefield) >= F.lit(date_low)) & 
                    (F.col(datefield) <= F.lit(date_high))).sort(F.col(datefield))
        df = df.toPandas()
        
    elif dataType == 'Pandas':
        
        # Check if the datefield is a datetime and convert it to a date if it is
        if df[datefield].dtype in ['datetime64[ns]']:
            df[datefield] = df[datefield].dt.date
            df[datefield] = df[datefield].dt.to_period(time_group_mapping[time_group]).dt.to_timestamp()

        elif df[datefield].dtype in ['int64', 'int32']:
            print("datefield is an integer, skipping conversion to date")
            datefieldIsNumeric = True

        if not count or (countField not in df.columns):
            df = df.groupby(groupFields).size().reset_index(name=countField).sort_values(by=countField, ascending=False)
            if plot_proportions and grouping is not None:
                total = df.groupby(grouping)[countField].transform('sum')
                df[countField] = df[countField] / total

        # Filter by date range
        if datefieldIsNumeric:
            df = df[(df[datefield] >= date_low) & (df[datefield] <= date_high)]
        else:
            df = df[(df[datefield] >= pd.to_datetime(date_low).date()) & (df[datefield] <= pd.to_datetime(date_high).date())]
            
    if grouping is not None:
        df[grouping] = df[grouping].astype(str) 
     # Convert the grouping column to string to ensure discrete colors
     
     # Convert the grouping column to string to ensure discrete colors
     
    if plot_proportions:
        ylab = 'Proportion'
    
    if plot_lib == 'plotnine':
       
        if datefieldIsNumeric:
            print("datefield is numeric")
            result = (pn.ggplot(df, pn.aes(**plot_arguments))
                + pn.geom_point()
                + pn.labs(title=title, y=ylab, x=xlab)
                + pn.theme(axis_text_x=pn.element_text(angle=90, hjust=1))
                + pn.scale_x_continuous()  # Modify this line
            )
        else:
            result = (pn.ggplot(df, pn.aes(**plot_arguments))
                + pn.geom_point()
                + pn.labs(title=title, y=ylab, x=xlab)
                + pn.theme(axis_text_x=pn.element_text(angle=90, hjust=1))
                + pn.scale_x_date(date_breaks=date_break, date_labels="%b %Y")
            )
            
        if useLog:
            print("log scale")
            result += pn.scale_y_log10()

        if surging_dates is not None:
            print("surging dates")
            if isinstance(df[datefield].dtype, np.number):
                # Handle surging_dates and annotate differently when datefield is a number
                result += pn.geom_vline(xintercept=surging_dates, linetype="dashed", color="red")
                result += pn.annotate("text", x=surging_dates, y=annotate_offset, label=surging_dates, 
                    angle = 90, size = 8)
        if grouping is not None:
            pass
        return result
    
        #result += scale_color_manual(values=df[grouping].astype('category').cat.codes)
    elif plot_lib == 'plotly':
        # Ensure the datefield is in datetime format for Plotly
        if not datefieldIsNumeric:
            df[datefield] = pd.to_datetime(df[datefield])
        
        # Create the scatter plot
        fig = px.scatter(df, x=datefield, y=countField, color=grouping if grouping is not None else None,
                        title=title, log_y=useLog, template="plotly_white")
        
        # Add vertical lines for surging dates
        if surging_dates:
            for date in surging_dates:
                fig.add_vline(x=date, line_dash="dash", line_color="red")
                # Optionally, add annotations for each surging date
                # Adjust the y-position of the annotation as needed
                fig.add_annotation(x=date, y=df[countField].max(), text=date, showarrow=True, arrowhead=1)
        
        # Update layout with axis titles
        fig.update_layout(xaxis_title=xlab, yaxis_title=ylab)
        
        # If using a log scale, this is already handled by the log_y parameter in px.scatter
        # Additional customization for plotly plots can be added here
        
        return fig

def plotByTime(df, datefield='date', xlab='', ylab='', title='', date_low='2019-01-01',
               surging_dates='COVID-19', count=None, date_break="2 year", date_high=None,
               annotate_offset=10, useLog = True, grouping = None, plot_proportions=False, 
               time_group='day', plot_lib='plotnine', geom_line = False):
    """
    Plots a scatter plot of counts over time with optional vertical lines.

    Parameters:
    df (pandas.DataFrame): The dataframe containing the data to plot.
    datefield (str, optional): The column name for the date values. Defaults to 'date'.
    xlab (str, optional): The label for the x-axis. Defaults to ''.
    ylab (str, optional): The label for the y-axis. Defaults to ''.
    title (str, optional): The title of the plot. Defaults to ''.
    date_low (str, optional): The lower limit for the date range. Defaults to '2019-01-01'.
    surging_dates (str or list, optional): A list of dates where vertical dashed lines should be drawn. Defaults to 'COVID-19'.
    count (str, optional): The column name for the count values. If None, the function will count the number of records for each date. Defaults to None.
    date_break (str, optional): The interval for the date breaks on the x-axis. Defaults to '2 year'.
    date_high (str, optional): The upper limit for the date range. If None, the function will use the current date. Defaults to None.
    annotate_offset (int, optional): The y-axis value for the annotations. Defaults to 10.
    useLog (bool, optional): Whether to use a log scale for the y-axis. Defaults to True.
    grouping (str, optional): The column name for the grouping variable. If provided, points will be colored by this variable. Defaults to None.
    plot_proportions (bool, optional): Whether to plot proportions instead of counts. Defaults to False.
    time_group (str, optional): The time unit for grouping the data. Allowed values are 'day', 'week', 'month', 'year'. Defaults to 'day'.

    Returns:
    plotnine.ggplot: A plotnine scatter plot.
    """
    # function body...
    
    # Check if time_group is one of the allowed values
    if time_group not in ['day', 'week', 'month', 'year']:
        logger.info(f"Error: Invalid time_group '{time_group}'. Allowed values are 'day', 'week', 'month', 'year'.")
        return
    
    # Map time_group to the corresponding values used by date_trunc and to_period
    time_group_mapping = {'day': 'D', 'week': 'W', 'month': 'M', 'year': 'Y'}
    
    countField         = count if count else 'count'
    datefieldIsNumeric = False
    plot_arguments     = {'x': datefield, 'y': countField}
    
    if surging_dates == 'COVID-19':
        surging_dates = ['2020-03-01', '2020-07-01', '2020-11-01', '2021-01-01', '2021-12-01', '2022-08-01']
    elif surging_dates == 'COVID-19 Vaccine':
        surging_dates = ['2020-12-14', '2021-04-19', '2021-08-13', '2021-09-20', '2021-11-19', '2022-03-29', '2022-09-02']
    else:
        logger.info(f"surging_dates = {surging_dates}")
        surging_dates = surging_dates
        
    if date_high is None:
        date_high = datetime.now().date()
        
    if grouping is not None:    
        plot_arguments['color'] = grouping
        groupFields = [F.col(datefield), F.col(grouping)]
    else:
        groupFields = [F.col(datefield)]
        
    
    if isinstance(df, DataFrame):
        dataType = 'Spark'
    elif isinstance(df, pd.DataFrame):
        dataType = 'Pandas'
    else:
        logger.info("Don't know the data type")
        # might need to exit.
        
    if dataType == 'Spark':
        
        # Check if the datefield is a number and convert it to a Date if it is
        # Check if the datefield is a number and convert it to a Date if it is
        if isinstance(df.schema[datefield].dataType, IntegerType):
            logger.info("datefield is an integer, skipping conversion to date")
            datefieldIsNumeric = True
        else:
            logger.info("converting to date")
            # Try to convert the datefield to a date
            df = df.withColumn('datefield_temp', F.to_date(F.col(datefield)))

            # Create a new column that is True when the conversion was successful and False otherwise
            df = df.withColumn('is_valid_date', F.when(F.col('datefield_temp').isNull(), False).otherwise(True))

            # Filter out the records where the conversion failed
            df = df.filter(F.col('is_valid_date'))

            # Drop the is_valid_date column
            df = df.drop('is_valid_date')
            df = df.drop(datefield)

            # Rename the temporary datefield column to the original column name
            df = df.withColumnRenamed('datefield_temp', datefield)
            
            # Group by time_group
            df = df.withColumn(datefield, F.date_trunc(time_group, F.col(datefield)))
            

        if not count or (countField not in df.columns):
            df = df.groupBy(groupFields).count().withColumnRenamed('count', countField)
            if plot_proportions and grouping is not None:
                window = Window.partitionBy(grouping)
                df = df.withColumn('total', F.sum(F.col(countField)).over(window))
                df = df.withColumn(countField, F.col(countField) / F.col('total'))
            df = df.sort(F.col(countField).desc())
            
        # Filter by date range
        df = df.filter((F.col(datefield) >= F.lit(date_low)) & 
                    (F.col(datefield) <= F.lit(date_high))).sort(F.col(datefield))
        df = df.toPandas()
        
    elif dataType == 'Pandas':
        
        # Check if the datefield is a datetime and convert it to a date if it is
        if df[datefield].dtype in ['datetime64[ns]']:
            df[datefield] = df[datefield].dt.date
            df[datefield] = df[datefield].dt.to_period(time_group_mapping[time_group]).dt.to_timestamp()

        elif df[datefield].dtype in ['int64', 'int32']:
            print("datefield is an integer, skipping conversion to date")
            datefieldIsNumeric = True

        if not count or (countField not in df.columns):
            df = df.groupby(groupFields).size().reset_index(name=countField).sort_values(by=countField, ascending=False)
            if plot_proportions and grouping is not None:
                total = df.groupby(grouping)[countField].transform('sum')
                df[countField] = df[countField] / total

        # Filter by date range
        if datefieldIsNumeric:
            df = df[(df[datefield] >= date_low) & (df[datefield] <= date_high)]
        else:
            df = df[(df[datefield] >= pd.to_datetime(date_low).date()) & (df[datefield] <= pd.to_datetime(date_high).date())]
            
    if grouping is not None:
        df[grouping] = df[grouping].astype(str) 
     # Convert the grouping column to string to ensure discrete colors
     
     # Convert the grouping column to string to ensure discrete colors
     
    if plot_proportions:
        ylab = 'Proportion'
    
    if plot_lib == 'plotnine':
       
        if datefieldIsNumeric:
            print("datefield is numeric")
            result = (
                  pn.ggplot(df, pn.aes(**plot_arguments))
                + pn.geom_point()
                + pn.labs(title=title, y=ylab, x=xlab)
                + pn.theme(axis_text_x=pn.element_text(angle=90, hjust=1))
                + pn.scale_x_continuous()  # Modify this line
            )
        else:
            result = (
                  pn.ggplot(df, pn.aes(**plot_arguments))
                + pn.geom_point()
                + pn.labs(title=title, y=ylab, x=xlab)
                + pn.theme(axis_text_x=pn.element_text(angle=90, hjust=1))
                + pn.scale_x_date(date_breaks=date_break, date_labels="%b %Y")
            )
            
        if geom_line:
            result = result + pn.geom_line()

        if useLog:
            logger.info("log scale")
            result += pn.scale_y_log10()

        if surging_dates is not None:
            logger.info("surging dates")
            if isinstance(df[datefield].dtype, np.number):
                # Handle surging_dates and annotate differently when datefield is a number
                result += pn.geom_vline(xintercept=surging_dates, linetype="dashed", color="red")
                result += pn.annotate("text", x=surging_dates, y=annotate_offset, label=surging_dates, 
                    angle = 90, size = 8)
        if grouping is not None:
            pass
        return result
    
        #result += scale_color_manual(values=df[grouping].astype('category').cat.codes)
    elif plot_lib == 'plotly':
        # Ensure the datefield is in datetime format for Plotly
        if not datefieldIsNumeric:
            df[datefield] = pd.to_datetime(df[datefield])

        # Count the number of observations per group
        group_counts = df[grouping].value_counts().reset_index()
        group_counts.columns = [grouping, countField]

        # Merge the counts back to the original DataFrame
        df = df.merge(group_counts, on=grouping)

        # Sort the DataFrame by the count
        df = df.sort_values(by=countField, ascending=False)
        
        # Create the scatter plot
        fig = px.scatter(df, x=datefield, y=countField, color=grouping if grouping is not None else None,
                        title=title, log_y=useLog, template="plotly_white")
        # Create the line plot
        line_fig = px.line(df, x=datefield, y=countField, color=grouping if grouping is not None else None)

        # Add the line plot to the scatter plot
        for trace in line_fig.data:
            fig.add_trace(trace)
        # Add vertical lines for surging dates
        if surging_dates:
            for date in surging_dates:
                fig.add_vline(x=date, line_dash="dash", line_color="red")
                # Optionally, add annotations for each surging date
                # Adjust the y-position of the annotation as needed
                fig.add_annotation(x=date, y=df[countField].max(), text=date, showarrow=True, arrowhead=1)
        
        # Update layout with axis titles
        fig.update_layout(xaxis_title=xlab, yaxis_title=ylab)
        
        # If using a log scale, this is already handled by the log_y parameter in px.scatter
        # Additional customization for plotly plots can be added here
        
        return fig


def plotByTimely(df, datefield='date', count=None, title='', date_low='2019-01-01', date_high=None, useLog = True, grouping = None):
    """
    Plots a scatter plot of counts over time with optional vertical lines.
    """
    # Check if datefield exists in the dataframe
    if datefield not in df.columns:
        print(f"Error: Column '{datefield}' does not exist in the dataframe.")
        return

    # Convert datefield to date if it's not already
    if isinstance(df.schema[datefield].dataType, IntegerType):
        print("datefield is an integer, skipping conversion to date")
    else:
        df = df.withColumn(datefield, F.to_date(F.col(datefield)))

    # Filter by date range
    if date_high is None:
        date_high = df.agg({datefield: 'max'}).collect()[0][0]
    else:
        date_high = pd.to_datetime(date_high)

    date_low = pd.to_datetime(date_low)

    df = df.filter((F.col(datefield) >= date_low) & (F.col(datefield) <= date_high))

    # Group by datefield and optionally by grouping
    if grouping is not None:
        df_grouped = df.groupby([datefield, grouping]).count()
    else:
        df_grouped = df.groupby(datefield).count()

    # Convert the grouped DataFrame to pandas for plotting
    df_grouped = df_grouped.toPandas()

    # Create the plot
    if grouping is not None:
        fig = px.scatter(df_grouped, x=datefield, y='count', color=grouping, title=title, log_y=useLog, hover_data=[datefield, 'count', grouping])
    else:
        fig = px.scatter(df_grouped, x=datefield, y='count', title=title, log_y=useLog, hover_data=[datefield, 'count'])

    return fig



def plotTopEntities(df,low_date, high_date, plot_date, num_entities, title, 
                    datefield, grouping = 'tenant', date_break = '10 year', time_group = 'year', 
                    plot_lib = 'plotnine', surging_dates = None, geom_line = False, plot_proportions = False):
    top_entities = (
        df
        .filter(F.col(datefield) > F.to_date(F.lit(low_date)))
        .filter(F.col(datefield) <= F.to_date(F.lit(high_date)))
        .groupby([grouping])
        .count()
        .sort([F.col('count').desc()])
        .limit(num_entities)
    )

    df = (
        df
        .filter(F.col(datefield) > F.to_date(F.lit(low_date)))
        .filter(F.col(datefield) <= F.to_date(F.lit(plot_date)))
        .join(top_entities, on = [grouping])
    )
    return(
        plotByTime(df, datefield = datefield, title = title, 
                grouping = grouping, date_break = date_break, time_group = time_group,  date_low=low_date, 
                 plot_lib = plot_lib, surging_dates = surging_dates, geom_line = geom_line, plot_proportions = plot_proportions)
    )

    