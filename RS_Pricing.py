import polars as pl
import pandas as pd
import requests
from os.path import exists
from pathlib import Path
import markdown2
# make data directory (if it does not exist)
data_directory = "data/"
Path(data_directory).mkdir(parents=True, exist_ok=True)
# make public directory (if it does not exist)
html_dir = "public/"
Path(html_dir).mkdir(parents=True, exist_ok=True)
## Query OSRS Mapping API
url = 'https://prices.runescape.wiki/api/v1/osrs/mapping'
headers = {
    'User-Agent': 'Major Discount Notification 0.1',
    'From': 'j.grant.redskins@gmail.com'
}
#  - Pull data for every item ID
#  - Normalize into Pandas DF
#  - Convert Pandas DF to Polars DF
#  - Declare column data types
df_map = pd.json_normalize(requests.get(url, headers=headers).json())
df_map = pl.DataFrame(df_map).with_columns(
    [
        pl.col("examine").cast(pl.Utf8), pl.col("id").cast(pl.Int64), pl.col("members").cast(pl.Boolean),
        pl.col("lowalch").cast(pl.Float64), pl.col("limit").cast(pl.Float64), pl.col("value").cast(pl.Int64),
        pl.col("highalch").cast(pl.Float64), pl.col("icon").cast(pl.Utf8), pl.col("name").cast(pl.Utf8),
    ]
)
## Query OSRS Timeseries API
#  - Identify Item IDs to query
#   - Only allowed to query a single ID per request
#   - Unneeded for output items being high-alched
#    - *but* query for items to be purchased solely for high alching profit
osrs_item_ids = [
    563, 562, 560, 565, 573, 569, 571, 1777, 5295, 257, 207, 99, 231, 139, 561, 575, 569, 571, 1515,
    5502, 5289, # palm [sappling|seed]
    5503, 5290, # calquat [sappling|seed]
    21480, 21488, # mahogany [sappling|seed]
    5501, 5288, # papaya [sappling|seed]
    5373, 5315 # yew [sappling|seed]
]
osrs_high_alch_item_ids = [
    1397, 1399, 1393, 1395, 855
]
base_url = 'https://prices.runescape.wiki/api/v1/osrs/timeseries?timestep=1h&id='
headers = {
    'User-Agent': 'Major Discount Notification 0.1',
    'From': 'j.grant.redskins@gmail.com'  # This is another valid field
}
#  - Pull data for every item ID
#    - Normalize into Pandas DF
#    - Convert Pandas DF to Polars DF
#    - Declare column data types
df_timeseries = pl.DataFrame()
# Query for each itemID in array/list, then declare data types
for item_id in osrs_item_ids:
    response = requests.get(
        base_url + str(item_id), headers=headers
    ) # add non-200 response handling
    df_timeseries = pl.concat(
        [
            df_timeseries, pl.DataFrame(
                pd.json_normalize(response.json(), record_path=['data'])
            ).lazy().with_columns(
                [pl.lit(item_id).alias('id')]
            ).with_columns(
                [
                    pl.col("timestamp").cast(pl.Int64), pl.col("avgHighPrice").cast(pl.Int64), pl.col("avgLowPrice").cast(pl.Int64),
                    pl.col("highPriceVolume").cast(pl.Int64), pl.col("lowPriceVolume").cast(pl.Int64), pl.col("id").cast(pl.Int64)
                ]
            ).with_columns(
                [(pl.col("timestamp") * 1e3).cast(pl.Datetime).dt.with_time_unit("ms").alias("datetime")]
            ).with_columns(
                [pl.col("datetime").dt.strftime(fmt="%Y-%m-%d %H").cast(pl.Utf8).alias("ymd_h")]
            ).collect()
        ], how = "diagonal", rechunk = True #vertical
    )
### Write Out Timeseries Data
# **Timeseries API RESTful service**: 
# "Gives a list of the high and low prices of item with the given id at the given interval, up to 365 maximum."
# If we want to do long-term trending, more than the 365 maximum, we need to incrementally grow the data by appending data written to "disk"
#  - Check if data (parquet file) already exists
#   - If exists:
#     - read in missing (historical data not included in data pull limitations, 365 1h records)
#  - Write data out to parquet file, zstd compression

parquest_path = data_directory + "osrs_timeseries_df.parquet"
parquet_exists = exists(parquest_path)
# if data (parquet) already exists, then add missing (historical) data to the data set
if parquet_exists:
    df_timeseries = pl.concat(
        [
            df_timeseries,
            # historical records saved off from previous run
            pl.scan_parquet(parquest_path).filter(
                pl.col("datetime") < df_timeseries.select("datetime").min()[0,0]
            ).collect()
        ], how = "vertical", rechunk = True
    )
# either write new (if file does not exist) or overwrite file with historical + new data
df_timeseries.write_parquet(parquest_path, compression = "zstd")
## Join Map & Timeseries Dataframes, Calculate Median & Most Recent Prices
df_results = df_timeseries.lazy().groupby("id").agg(
    pl.col("avgHighPrice").median()
).rename({"avgHighPrice": "med_avgHighPrice"}).join(
    df_timeseries.lazy().groupby("id").agg(
        [
            pl.all().sort_by('datetime').last()
        ]
    ).rename(
        {"avgHighPrice": "last_avgHighPrice", "avgLowPrice": "last_avgLowPrice"}
    ).select(["id", "last_avgHighPrice", "last_avgLowPrice"]),
    on="id", how="inner"
).join(
    df_map.lazy().filter(pl.col('id').is_in((osrs_item_ids + osrs_high_alch_item_ids))).select(
        ["id", "highalch", "members", "name"]
    ),
    on="id", how="inner"
).collect()
### Helper functions for generating buy/no-buy markdown
def gen_staff_markdown(df_map, df_results, x_battlestaff_id, orb_id, nature_id = 561, battlestaff_price = 7000):
    df_orb = df_results.lazy().filter(pl.col("id") == orb_id).with_columns(
        [
            (
                (
                    # X battlestaff high alch price - (nature rune price + battlestaff_price)
                    df_map.filter(pl.col("id") == x_battlestaff_id).select("highalch")[0,0] - 
                    (df_results.filter(pl.col("id") == nature_id).select("last_avgHighPrice")[0,0] + battlestaff_price)
                ) - pl.col("last_avgHighPrice")
            ).alias("profit"),
            (
                pl.col("last_avgHighPrice")/pl.col("med_avgHighPrice")
            ).round(2).alias("discount")
        ]
    ).with_columns(
        [
            pl.when(
                    pl.col("profit") >= 0
                ).then("Buy").otherwise("Don't Buy").alias("if_Buy_str"),
            pl.when(pl.col("profit") >= 0).then(
                pl.concat_str(
                    [
                        pl.lit(" between"),
                        pl.col("last_avgLowPrice"),
                        pl.lit("and"),
                        pl.col("last_avgHighPrice")
                    ], sep = " "
                )
            ).otherwise("").alias("extra_str")
        ]
    ).collect()
    orb = (
        "### " + df_orb.select("name")[0,0] + ": \n***" + str(df_orb.select("if_Buy_str")[0,0]) + "***" +
        str(df_orb.select("extra_str")[0,0]) + ".  " +
        "**Profit**: *" + str(df_orb.select("profit")[0,0]) + "* " +
        "**Value**: *" + str(df_orb.select("discount")[0,0]) + "x against historical median*"
    )
    return(orb)

def gen_bow_markdown(df_map, df_results, x_bow_id, log_id, nature_id = 561, bowstring_id = 1777):
    df_bowstring = df_results.lazy().filter(pl.col("id") == bowstring_id).with_columns(
        [
            (
                (
                    # X bow high alch price - (nature rune price + bowstring price + log price)
                    df_map.filter(pl.col("id") == x_bow_id).select("highalch")[0,0] - 
                    (
                        df_results.filter(pl.col("id") == nature_id).select("last_avgHighPrice")[0,0] + 
                        df_results.filter(pl.col("id") == log_id).select("last_avgHighPrice")[0,0]
                    )
                ) - pl.col("last_avgHighPrice")
            ).alias("profit"),
            (
                pl.col("last_avgHighPrice")/pl.col("med_avgHighPrice")
            ).round(2).alias("discount")
        ]
    ).with_columns(
        [
            pl.when(
                    pl.col("profit") >= 0
                ).then("Buy").otherwise("Don't Buy").alias("if_Buy_str"),
            pl.when(pl.col("profit") >= 0).then(
                pl.concat_str(
                    [
                        pl.lit(" between"),
                        pl.col("last_avgLowPrice"),
                        pl.lit("and"),
                        pl.col("last_avgHighPrice")
                    ], sep = " "
                )
            ).otherwise("").alias("extra_str")
        ]
    ).collect()
    bow_string = (
        "### " + df_bowstring.select("name")[0,0] + ": \n***" + str(df_bowstring.select("if_Buy_str")[0,0]) + "***" +
        str(df_bowstring.select("extra_str")[0,0]) + ".  " +
        "**Profit**: *" + str(df_bowstring.select("profit")[0,0]) + "* " +
        "**Value**: *" + str(df_bowstring.select("discount")[0,0]) + "x against historical median*"
    )
    return(bow_string)
def gen_sappling_markdown(df_map, df_results, x_sappling_id):
    seed_lookup = {
        5502: 5289, 5503: 5290, 21480: 21488,
        5501: 5288, 5373: 5315
    }

    df_sappling = df_results.filter(pl.col("id") == x_sappling_id)

    profit = (
        df_results.filter(pl.col("id") == x_sappling_id).select(
            pl.min(["last_avgLowPrice", "last_avgHighPrice", "med_avgHighPrice"])
        ).item()
        -
        df_results.filter(pl.col("id") == seed_lookup[x_sappling_id]).select(
            pl.min(["last_avgLowPrice", "last_avgHighPrice", "med_avgHighPrice"])
        ).item()
    )

    sappling = (
        "### " + df_sappling.select("name")[0,0] + ": \n" +
        "**Profit**: *" + str(profit) + "* "
    )

    return(sappling)
## Generate HTML from Markdown
# Write out to HTML file for publishing via GitHub Pages
open((html_dir + "output.html"), "w").write(
    markdown2.markdown(
        "\n***\n" + 
        gen_staff_markdown(df_map, df_results, x_battlestaff_id = 1397, orb_id = 573) + "\n" + "\n***\n" + 
        gen_staff_markdown(df_map, df_results, x_battlestaff_id = 1399, orb_id = 575) + "\n" + "\n***\n" + 
        gen_staff_markdown(df_map, df_results, x_battlestaff_id = 1393, orb_id = 569) + "\n" + "\n***\n" + 
        gen_staff_markdown(df_map, df_results, x_battlestaff_id = 1395, orb_id = 571) + "\n\n" + "\n***\n" + 
        gen_bow_markdown(df_map, df_results, x_bow_id = 855, log_id = 1515, nature_id = 561, bowstring_id = 1777) + "\n***\n" +
        gen_sappling_markdown(df_map, df_results, x_sappling_id = 5502) + "\n***\n" +
        gen_sappling_markdown(df_map, df_results, x_sappling_id = 5503) + "\n***\n" +
        gen_sappling_markdown(df_map, df_results, x_sappling_id = 21480) + "\n***\n" +
        gen_sappling_markdown(df_map, df_results, x_sappling_id = 5501) + "\n***\n" +
        gen_sappling_markdown(df_map, df_results, x_sappling_id = 5373) + "\n***\n"
    )
)
# EOF