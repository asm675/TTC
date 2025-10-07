import os
from pyspark.sql import SparkSession, functions as F, types as T, Window as W


def get_cfg():
    return {
        "BROKER": os.getenv("BROKER", "redpanda:9092"),
        "MONGODB_URI": os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        "DB_NAME": os.getenv("MONGO_DB", "transit_project"),
        "RAW_COLL": os.getenv("RAW_COLL", "ttc_raw_snapshots"),
        "DER_COLL": os.getenv("DER_COLL", "departures"),
        "MET_COLL": os.getenv("MET_COLL", "departure_metrics"),
        "CHK_DIR": os.getenv("CHK_DIR", os.path.abspath("./chk")),
        "TOPIC": "ttc.raw.snapshots",
    }


def build_spark():
    spark = (
        SparkSession.builder.appName("ttc-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, broker, topic):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("kafka.security.protocol", "PLAINTEXT")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def schemas():
    """
    Define nested Spark schemas for parsing transit departure data.

        Creates a hierarchical schema structure that matches the JSON format of transit
        data containing stations, stops, routes, and scheduled departure times.

        Returns:
            StructType: The outer schema with fields:
                - snapshot_ts (LongType): Unix timestamp when the data snapshot was taken
                - station_uri (StringType): Station identifier URI
                - payload (StructType): Nested structure containing:
                    - uri (StringType): Payload URI identifier
                    - time (LongType): Payload timestamp
                    - stops (ArrayType): Array of stop objects, each containing:
                        - uri (StringType): Stop identifier
                        - routes (ArrayType): Array of route objects, each containing:
                            - uri (StringType): Route identifier
                            - name (StringType): Route display name
                            - stop_times (ArrayType): Array of scheduled times, each with:
                                - departure_timestamp (LongType): Unix departure timestamp
                                - shape (StringType): Route shape/direction identifier
    """
    schema = T.StructType(
        [
            T.StructField("uri", T.StringType(), True),
            T.StructField("time", T.LongType(), True),
            T.StructField(
                "stops",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("uri", T.StringType(), True),
                            T.StructField(
                                "routes",
                                T.ArrayType(
                                    T.StructType(
                                        [
                                            T.StructField("uri", T.StringType(), True),
                                            T.StructField("name", T.StringType(), True),
                                            T.StructField(
                                                "stop_times",
                                                T.ArrayType(
                                                    T.StructType(
                                                        [
                                                            T.StructField(
                                                                "departure_timestamp",
                                                                T.LongType(),
                                                                True,
                                                            ),
                                                            T.StructField(
                                                                "shape",
                                                                T.StringType(),
                                                                True,
                                                            ),
                                                        ]
                                                    )
                                                ),
                                            ),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    )
                ),
            ),
        ]
    )
    outer_schema = T.StructType(
        [
            T.StructField("snapshot_ts", T.LongType(), True),
            T.StructField("station_uri", T.StringType(), True),
            T.StructField("payload", schema, True),
        ]
    )
    return outer_schema


def parse_stream(raw_df, outer_schema):
    """
    Parse raw streaming transit data into structured departure records.

    Takes a raw DataFrame from a streaming source and transforms it into two DataFrames:
    one preserving the raw JSON and another with parsed, flattened departure information.
    The function explodes nested arrays of stops, routes, and stop_times, filters for
    subway routes, and converts timestamps to Toronto timezone.

    Args:
        raw_df: Input DataFrame with a 'value' column containing JSON transit data.
        outer_schema: Spark StructType schema defining the structure of the JSON payload,
                     expected to contain snapshot_ts, station_uri, and nested payload data.

    Returns:
        tuple: A pair of DataFrames (raw_json, df) where:
            - raw_json: DataFrame with columns 'raw_json' (string) and 'ingest_ts' (timestamp).
            - df: Flattened DataFrame with columns:
                - uri: Transit URI identifier
                - time: Original time field
                - snapshot_ts: Unix timestamp of the snapshot
                - snapshot_dt: Timestamp converted to America/Toronto timezone
                - station_uri: Station identifier
                - stop_uri: Stop identifier
                - route_uri: Route identifier (filtered to end with '_subway')
                - route_name: Human-readable route name
                - dep_ts: Departure timestamp
                - shape: Route shape identifier
                - ts: Processing timestamp
    """

    raw_json = raw_df.select(
        F.col("value").cast("string").alias("raw_json"),
        F.current_timestamp().alias("ingest_ts"),
    )
    # departures (derived)
    parsed = F.from_json(F.col("value").cast("string"), outer_schema).alias("msg")
    df = (
        raw_df.select(parsed)
        .select("msg.snapshot_ts", "msg.station_uri", "msg.payload.*")
        .select(
            "uri",
            "time",
            "snapshot_ts",
            "station_uri",
            F.explode("stops").alias("stop"),
        )
        .select(
            "uri",
            "time",
            "snapshot_ts",
            "station_uri",
            F.col("stop.uri").alias("stop_uri"),
            F.explode("stop.routes").alias("route"),
        )
        .filter(F.col("route.uri").endswith("_subway"))
        .select(
            "uri",
            "time",
            "snapshot_ts",
            "station_uri",
            "stop_uri",
            F.col("route.uri").alias("route_uri"),
            F.col("route.name").alias("route_name"),
            F.explode("route.stop_times").alias("st"),
        )
        .select(
            "uri",
            "time",
            "snapshot_ts",
            F.from_utc_timestamp(
                F.from_unixtime(F.col("snapshot_ts")), F.lit("America/Toronto")
            ).alias("snapshot_dt"),
            "station_uri",
            "stop_uri",
            "route_uri",
            "route_name",
            F.col("st.departure_timestamp").alias("dep_ts"),
            F.col("st.shape").alias("shape"),
            F.current_timestamp().alias("ts"),
        )
    )
    return raw_json, df


def write_mongo_stream(df, uri, db, coll, chk_dir):
    return (
        df.writeStream.format("mongodb")
        .option("spark.mongodb.connection.uri", uri)
        .option("spark.mongodb.database", db)
        .option("spark.mongodb.collection", coll)
        .option("checkpointLocation", chk_dir)
        .start()
    )


def start_metrics_stream(departures_df, cfg):
    """
    Start a streaming query that computes headway metrics and anomaly detection for departures.

    For each micro-batch of departure data, this function:
    1. Calculates headway (time between consecutive departures) per route and station.
    2. Computes rolling statistics (mean, std dev) over the last K departures.
    3. Performs z-score based anomaly detection (|z| > 3).
    4. Appends the enriched metrics to MongoDB.

    Args:
        departures_df: Streaming DataFrame containing departure records with columns:
                      station_uri, route_uri, dep_ts, snapshot_ts, snapshot_dt.
        cfg: Configuration dictionary containing:
            - MONGODB_URI: MongoDB connection string
            - DB_NAME: Target database name
            - MET_COLL: Metrics collection name
            - CHK_DIR: Checkpoint directory path for streaming state

    Returns:
        StreamingQuery: The active streaming query object. Call .awaitTermination()
                       or .stop() to manage the stream lifecycle.
    """

    def process_batch(batch_df, batch_id: int):
        w = W.partitionBy("route_uri", "station_uri").orderBy("dep_ts")
        dep2 = batch_df.withColumn(
            "headway_sec",
            F.col("dep_ts").cast("long") - F.lag(F.col("dep_ts").cast("long")).over(w),
        ).withColumn("headway_mins", F.round(F.col("headway_sec") / 60.0, 2))

        # 2) Rolling stats (last K rows) within THIS batch
        K = 10
        w_k = (
            W.partitionBy("route_uri", "station_uri")
            .orderBy("dep_ts")
            .rowsBetween(-K, -1)
        )
        metrics = (
            dep2.withColumn("roll_mean", F.avg("headway_mins").over(w_k))
            .withColumn("roll_std", F.stddev("headway_mins").over(w_k))
            .withColumn(
                "z",
                F.when(
                    F.col("roll_std").isNull() | (F.col("roll_std") == 0), F.lit(None)
                ).otherwise(
                    (F.col("headway_mins") - F.col("roll_mean")) / F.col("roll_std")
                ),
            )
            .withColumn("anomaly", F.abs(F.col("z")) > F.lit(3))
            .select(
                "station_uri",
                "route_uri",
                "dep_ts",
                "headway_mins",
                "roll_mean",
                "roll_std",
                "z",
                "anomaly",
                "snapshot_ts",
                "snapshot_dt",
            )
        )
        (
            metrics.write.format("mongodb")
            .option("spark.mongodb.connection.uri", cfg["MONGODB_URI"])
            .option("spark.mongodb.database", cfg["DB_NAME"])
            .option("spark.mongodb.collection", cfg["MET_COLL"])
            .mode("append")
            .save()
        )

    return (
        departures_df.writeStream.option(
            "checkpointLocation", os.path.join(cfg["CHK_DIR"], "metrics")
        )
        .foreachBatch(process_batch)
        .start()
    )


def main():
    cfg = get_cfg()
    spark = build_spark()
    outer = schemas()

    raw_df = read_kafka_stream(spark, cfg["BROKER"], cfg["TOPIC"])
    raw_json, departures = parse_stream(raw_df, outer)

    write_mongo_stream(
        departures,
        cfg["MONGODB_URI"],
        cfg["DB_NAME"],
        cfg["DER_COLL"],
        os.path.join(cfg["CHK_DIR"], "derived_direct"),
    )
    write_mongo_stream(
        raw_json,
        cfg["MONGODB_URI"],
        cfg["DB_NAME"],
        cfg["RAW_COLL"],
        os.path.join(cfg["CHK_DIR"], "raw_direct"),
    )

    start_metrics_stream(departures, cfg)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
