from pyspark.sql import SparkSession, DataFrame


def load_dim_table():
    """Load dim table"""


def main():
    """Run main function"""
    spark: SparkSession = (
        SparkSession.builder.appName("Category Transformation")
        .getOrCreate()
    )

    # spark -> delta table


if __name__ == "__main__":
    main()
