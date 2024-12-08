# from kedro.framework.hooks import hook_impl
# from pyspark import SparkConf
# from pyspark.sql import SparkSession


# class SparkHooks:
#     @hook_impl
#     def after_context_created(self, context) -> None:
#         """Initialises a SparkSession using the config
#         defined in project's conf folder.
#         """

#         # Load the spark configuration in spark.yaml using the config loader
#         parameters = context.config_loader["spark"]
#         spark_conf = SparkConf().setAll(parameters.items())

#         # Initialise the spark session
#         spark_session_conf = (
#             SparkSession.builder.appName(context.project_path.name)
#             .enableHiveSupport()
#             .config(conf=spark_conf)
#         )
#         _spark_session = spark_session_conf.getOrCreate()
#         _spark_session.sparkContext.setLogLevel("WARN")

from kedro.framework.hooks import hook_impl
from pyspark import SparkConf
from pyspark.sql import SparkSession
import os

class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initializes a SparkSession using the config
        defined in project's conf folder.
        """
        # Load configuration from YAML files
        config = context.config_loader

        # Get the Java and JDBC driver paths from config
        java_home = config["spark"]["java_home"]
        postgresql_jar_path = config["spark"]["jdbc_driver"]

        # Set JAVA_HOME environment variable programmatically
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = f"{os.environ['JAVA_HOME']}\\bin;{os.environ.get('PATH', '')}"

        # Load the Spark configuration from spark.yml using the config loader
        parameters = config["spark"]
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialize the Spark session with the JDBC driver
        spark_session_conf = (
            SparkSession.builder.appName(context.project_path.name)
            .enableHiveSupport()
            .config(conf=spark_conf)
            .config("spark.jars", postgresql_jar_path)  # Specify the PostgreSQL JAR
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")

