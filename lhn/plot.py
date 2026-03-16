"""
lhn/plot.py

Visualization functions restored from v0.1.0.
These provide healthcare-specific plotting capabilities using plotnine
and plotly, with built-in support for:
- Time-series analysis with COVID surge date annotations
- Log-scale count plots
- Top-entity filtering and comparison
- Both Spark and pandas DataFrame input

Restored: 2026-03-15 from v0.1.0-monolithic branch.
"""

from lhn.header import (
    F, pd, DataFrame, Window, get_logger
)
from datetime import datetime
import numpy as np

logger = get_logger(__name__)

# Optional imports — plotnine and plotly may not be available on all environments
try:
    import plotnine as pn
except ImportError:
    pn = None

try:
    import plotly.express as px
except ImportError:
    px = None

try:
    from pyspark.sql.types import IntegerType
except ImportError:
    IntegerType = None


def count(df, fieldName):
    """Group by field and count with log transform.

    Args:
        df: Spark DataFrame
        fieldName (str): Column to group by

    Returns:
        pandas DataFrame with count and countln columns
    """
    result = (
        df.groupby(F.col(fieldName))
        .count()
        .withColumn('countln', F.log(F.col('count')))
        .toPandas()
    )
    return result


def plot_counts(df, x, y, xlab='', ylab='', title='', vlines=None,
                hlines=None, grouping=None):
    """Scatter plot of counts with log scales and optional reference lines.

    Args:
        df: pandas DataFrame
        x (str): X-axis column
        y (str): Y-axis column
        xlab (str): X-axis label
        ylab (str): Y-axis label
        title (str): Plot title
        vlines (list): X-axis values for vertical dashed lines
        hlines (list): Y-axis values for horizontal dashed lines
        grouping (str): Column for color grouping

    Returns:
        plotnine ggplot object
    """
    if pn is None:
        logger.warning("plotnine not available — cannot create plot")
        return None

    if vlines is None:
        vlines = []
    if hlines is None:
        hlines = []

    plot_arguments = {'x': x, 'y': y}
    if grouping is not None:
        df[grouping] = pd.Categorical(df[grouping])
        plot_arguments['color'] = grouping

    plot_result = (
        pn.ggplot(df, pn.aes(**plot_arguments))
        + pn.geom_point()
        + pn.scale_x_log10()
        + pn.scale_y_log10()
        + pn.labs(title=title, y=ylab, x=xlab)
    )
    for line in vlines:
        plot_result += pn.geom_vline(
            xintercept=line, linetype="dashed", color="red"
        )
    for line in hlines:
        plot_result += pn.geom_hline(
            yintercept=line, linetype="dashed", color="blue"
        )
    return plot_result


def _resolve_surging_dates(surging_dates):
    """Resolve named surging date presets to date lists."""
    if surging_dates == 'COVID-19':
        return [
            '2020-03-01', '2020-07-01', '2020-11-01',
            '2021-01-01', '2021-12-01', '2022-08-01'
        ]
    elif surging_dates == 'COVID-19 Vaccine':
        return [
            '2020-12-14', '2021-04-19', '2021-08-13',
            '2021-09-20', '2021-11-19', '2022-03-29', '2022-09-02'
        ]
    return surging_dates


def plotByTime(df, datefield='date', xlab='', ylab='', title='',
               date_low='2019-01-01', surging_dates='COVID-19', count=None,
               date_break="2 year", date_high=None, annotate_offset=10,
               useLog=True, grouping=None, plot_proportions=False,
               time_group='day', plot_lib='plotnine', geom_line=False):
    """Scatter plot of counts over time with COVID surge annotations.

    Handles both Spark and pandas DataFrames. Supports day/week/month/year
    time grouping, log scale, proportions mode, and both plotnine and plotly
    rendering backends.

    Args:
        df: Spark or pandas DataFrame
        datefield (str): Date column name
        xlab (str): X-axis label
        ylab (str): Y-axis label
        title (str): Plot title
        date_low (str): Lower date limit (YYYY-MM-DD)
        surging_dates: 'COVID-19', 'COVID-19 Vaccine', list of dates, or None
        count (str): Pre-computed count column (if None, counts are computed)
        date_break (str): X-axis date break interval (e.g., '2 year')
        date_high (str): Upper date limit (None = today)
        annotate_offset (int): Y position for surge date annotations
        useLog (bool): Use log scale for Y axis
        grouping (str): Column for color grouping
        plot_proportions (bool): Plot proportions instead of raw counts
        time_group (str): 'day', 'week', 'month', or 'year'
        plot_lib (str): 'plotnine' or 'plotly'
        geom_line (bool): Add line geometry to plotnine plot

    Returns:
        plotnine ggplot or plotly Figure
    """
    if time_group not in ['day', 'week', 'month', 'year']:
        logger.error(
            f"Invalid time_group '{time_group}'. "
            f"Allowed: 'day', 'week', 'month', 'year'."
        )
        return None

    time_group_mapping = {'day': 'D', 'week': 'W', 'month': 'M', 'year': 'Y'}

    countField = count if count else 'count'
    datefieldIsNumeric = False
    plot_arguments = {'x': datefield, 'y': countField}

    surging_dates = _resolve_surging_dates(surging_dates)

    if date_high is None:
        date_high = datetime.now().date()

    if grouping is not None:
        plot_arguments['color'] = grouping
        groupFields = [F.col(datefield), F.col(grouping)]
    else:
        groupFields = [F.col(datefield)]

    # Determine DataFrame type
    if isinstance(df, DataFrame):
        dataType = 'Spark'
    elif isinstance(df, pd.DataFrame):
        dataType = 'Pandas'
    else:
        logger.warning("Unknown DataFrame type")
        return None

    # --- Spark path ---
    if dataType == 'Spark':
        if (IntegerType is not None
                and isinstance(df.schema[datefield].dataType, IntegerType)):
            datefieldIsNumeric = True
        else:
            df = df.withColumn('datefield_temp', F.to_date(F.col(datefield)))
            df = df.withColumn(
                'is_valid_date',
                F.when(F.col('datefield_temp').isNull(), False)
                .otherwise(True)
            )
            df = df.filter(F.col('is_valid_date'))
            df = df.drop('is_valid_date').drop(datefield)
            df = df.withColumnRenamed('datefield_temp', datefield)
            df = df.withColumn(
                datefield, F.date_trunc(time_group, F.col(datefield))
            )

        if not count or (countField not in df.columns):
            df = (
                df.groupBy(groupFields)
                .count()
                .withColumnRenamed('count', countField)
            )
            if plot_proportions and grouping is not None:
                window = Window.partitionBy(grouping)
                df = df.withColumn(
                    'total', F.sum(F.col(countField)).over(window)
                )
                df = df.withColumn(
                    countField, F.col(countField) / F.col('total')
                )
            df = df.sort(F.col(countField).desc())

        df = (
            df.filter(
                (F.col(datefield) >= F.lit(date_low))
                & (F.col(datefield) <= F.lit(date_high))
            )
            .sort(F.col(datefield))
            .toPandas()
        )

    # --- Pandas path ---
    elif dataType == 'Pandas':
        if df[datefield].dtype in ['datetime64[ns]']:
            df[datefield] = df[datefield].dt.date
            df[datefield] = (
                df[datefield].dt
                .to_period(time_group_mapping[time_group])
                .dt.to_timestamp()
            )
        elif df[datefield].dtype in ['int64', 'int32']:
            datefieldIsNumeric = True

        if not count or (countField not in df.columns):
            group_cols = (
                [datefield, grouping] if grouping else [datefield]
            )
            df = (
                df.groupby(group_cols)
                .size()
                .reset_index(name=countField)
                .sort_values(by=countField, ascending=False)
            )
            if plot_proportions and grouping is not None:
                total = df.groupby(grouping)[countField].transform('sum')
                df[countField] = df[countField] / total

        if datefieldIsNumeric:
            df = df[
                (df[datefield] >= date_low) & (df[datefield] <= date_high)
            ]
        else:
            df = df[
                (df[datefield] >= pd.to_datetime(date_low).date())
                & (df[datefield] <= pd.to_datetime(date_high).date())
            ]

    if grouping is not None:
        df[grouping] = df[grouping].astype(str)

    if plot_proportions:
        ylab = 'Proportion'

    # --- plotnine rendering ---
    if plot_lib == 'plotnine':
        if pn is None:
            logger.warning("plotnine not available")
            return None

        if datefieldIsNumeric:
            result = (
                pn.ggplot(df, pn.aes(**plot_arguments))
                + pn.geom_point()
                + pn.labs(title=title, y=ylab, x=xlab)
                + pn.theme(
                    axis_text_x=pn.element_text(angle=90, hjust=1)
                )
                + pn.scale_x_continuous()
            )
        else:
            result = (
                pn.ggplot(df, pn.aes(**plot_arguments))
                + pn.geom_point()
                + pn.labs(title=title, y=ylab, x=xlab)
                + pn.theme(
                    axis_text_x=pn.element_text(angle=90, hjust=1)
                )
                + pn.scale_x_date(
                    date_breaks=date_break, date_labels="%b %Y"
                )
            )

        if geom_line:
            result = result + pn.geom_line()

        if useLog:
            result += pn.scale_y_log10()

        if surging_dates is not None and not datefieldIsNumeric:
            for date_str in surging_dates:
                try:
                    date_val = pd.to_datetime(date_str).date()
                    result += pn.geom_vline(
                        xintercept=date_val,
                        linetype="dashed", color="red", alpha=0.5
                    )
                except Exception:
                    pass

        return result

    # --- plotly rendering ---
    elif plot_lib == 'plotly':
        if px is None:
            logger.warning("plotly not available")
            return None

        if not datefieldIsNumeric:
            df[datefield] = pd.to_datetime(df[datefield])

        fig = px.scatter(
            df, x=datefield, y=countField,
            color=grouping if grouping is not None else None,
            title=title, log_y=useLog, template="plotly_white"
        )

        if geom_line:
            line_fig = px.line(
                df, x=datefield, y=countField,
                color=grouping if grouping is not None else None
            )
            for trace in line_fig.data:
                fig.add_trace(trace)

        if surging_dates:
            for date in surging_dates:
                fig.add_vline(x=date, line_dash="dash", line_color="red")
                fig.add_annotation(
                    x=date, y=df[countField].max(),
                    text=date, showarrow=True, arrowhead=1
                )

        fig.update_layout(xaxis_title=xlab, yaxis_title=ylab)
        return fig

    return None


def plotTopEntities(df, low_date, high_date, plot_date, num_entities,
                    title, datefield, grouping='tenant',
                    date_break='10 year', time_group='year',
                    plot_lib='plotnine', surging_dates=None,
                    geom_line=False, plot_proportions=False):
    """Filter to top N entities by count and plot their time series.

    Args:
        df: Spark DataFrame
        low_date (str): Start date for entity selection
        high_date (str): End date for entity selection
        plot_date (str): End date for the plot
        num_entities (int): Number of top entities to show
        title (str): Plot title
        datefield (str): Date column
        grouping (str): Entity grouping column (default: 'tenant')
        date_break (str): X-axis break interval
        time_group (str): Time aggregation unit
        plot_lib (str): 'plotnine' or 'plotly'
        surging_dates: Surge date annotations
        geom_line (bool): Add line geometry
        plot_proportions (bool): Plot proportions

    Returns:
        plotnine ggplot or plotly Figure
    """
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
        .join(top_entities, on=[grouping])
    )

    return plotByTime(
        df, datefield=datefield, title=title,
        grouping=grouping, date_break=date_break,
        time_group=time_group, date_low=low_date,
        plot_lib=plot_lib, surging_dates=surging_dates,
        geom_line=geom_line, plot_proportions=plot_proportions
    )
