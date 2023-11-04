import polars as pl
import os

PRJ_DIR = os.path.dirname(os.path.abspath(__file__))
CLIMBING_DATA_PATH = os.path.join(
    PRJ_DIR, "data", "Intermediate_Climbing_trips_10_years-2023-11-03-13-36-35.xlsx"
)

def create_climbing_dataset(df_with_type: pl.DataFrame, filter: str) -> pl.DataFrame:
    ice_df = df_with_type.filter(df_with_type["type"] == filter)
    ice_climb_by_year_branch = (
        ice_df.groupby(["year", "branch"])
        .agg(
            pl.count().alias("count_of_climbs"),
            pl.sum("participants_plus_leaders").alias(
                "sum_total_registered_participants"
            ),
        )
        .sort("branch", "year")
    )

    successful_registered_participants = (
        ice_df.filter(pl.col("result") == "Successful")
        .groupby(["year", "branch"])
        .agg(
            pl.sum("participants_plus_leaders").alias(
                "sum_successful_registered_participants"
            )
        )
        .sort("branch", "year")
    )

    ice_climb_by_year_branch = ice_climb_by_year_branch.join(
        successful_registered_participants, on=["year", "branch"]
    ).with_columns(
        success_ratio=(
            pl.col("sum_successful_registered_participants")
            / pl.col("sum_total_registered_participants")
        ).round(2)
    )
    return ice_climb_by_year_branch


if __name__ == "__main__":
    df = pl.read_excel(source=CLIMBING_DATA_PATH, sheet_name="data")
    route_types = pl.read_excel(source=CLIMBING_DATA_PATH, sheet_name="mapping")
    df_with_type = df.join(route_types, on="activity", how="inner")
    # Parse dates, get unique data, and add 1 to registered participants to account for 
    # leader
    df_with_type = (
        df_with_type.with_columns(
            start_date=pl.col("start_date").str.to_date(format="%Y-%m-%d")
        )
        .with_columns(year=pl.col("start_date").dt.year())
        .unique(["route", "start_date"])
        .with_columns(participants_plus_leaders=pl.col("registered_participants") + 1)
    )

    ice_climb_by_year_branch = create_climbing_dataset(
        df_with_type=df_with_type, filter="Ice"
    )
    ice_climb_by_year_branch.write_csv(file="result/ice_climb_by_year_branch.csv")

    ice_cragging_by_year_branch = create_climbing_dataset(
        df_with_type=df_with_type, filter="Ice cragging"
    )
    ice_cragging_by_year_branch.write_csv(file="result/ice_cragging_by_year_branch.csv")

    rock_by_year_branch = create_climbing_dataset(
        df_with_type=df_with_type, filter="Rock"
    )
    rock_by_year_branch.write_csv(file="result/rock_by_year_branch.csv")
