from __future__ import annotations
import os
import sys
import platform
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def _ensure_windows_hadoop() -> None:

    """
    - checks that winutils.exe exists
    - sets HADOOP_HOME and PATH so Spark can start
    """
    
    if platform.system() != "Windows":
        return

    # Project root: energy-data-pipeline/
    project_root = Path(__file__).resolve().parents[2]
    # Expected Hadoop folder inside the project
    hadoop_home = project_root / "hadoop"
    winutils = hadoop_home / "bin" / "winutils.exe"

    if not winutils.exists():
        raise FileNotFoundError(
            f"winutils.exe not found at: {winutils}\n"
            f"Expected path: <project_root>/hadoop/bin/winutils.exe"
        )

    # Required environment variables for Spark on Windows
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")


def get_spark_session(
    app_name: str = "energy-data-pipeline",
    master: str = "local[1]",
    log_level: str = "ERROR",
) -> SparkSession:
    
    """
    Create and return a SparkSession configured for:
    - local development
    - Delta Lake support
    - Windows stability
    """
    # Ensure Windows Hadoop setup
    _ensure_windows_hadoop()

    # Force Spark driver and workers to use the current virtualenv Python
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Local directories used by Spark
    local_tmp = r"C:\spark-tmp"
    warehouse_dir = r"C:\spark-warehouse"
    os.makedirs(local_tmp, exist_ok=True)
    os.makedirs(warehouse_dir, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Spark local storage
        .config("spark.local.dir", local_tmp)
        .config("spark.sql.warehouse.dir", warehouse_dir)

        # Ensure consistent Python interpreter
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)

        # Enable Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

         # Optimized for small local runs
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")

        # Reduce Windows temp-folder cleanup errors
        .config("spark.worker.cleanup.enabled", "false")
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "false")
    )

    # Create Spark session with Delta support
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # Set Spark log level
    spark.sparkContext.setLogLevel(log_level)

    # quick proof Delta is active
    print("✅ Spark started | Delta enabled =", "delta" in spark.conf.get("spark.sql.extensions", ""))
    return spark

