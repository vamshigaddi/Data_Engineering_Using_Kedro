"""
This is a boilerplate pipeline 'data_cleaning'
generated using Kedro 0.19.10
"""
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when,lower,trim, lit, to_date,regexp_replace, initcap
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    A class to clean various healthcare datasets.

    This class provides methods to clean datasets such as patients, medications,
    symptoms, conditions, and encounters by applying transformations like renaming
    columns, filling null values, dropping columns, and more.
    """

    @staticmethod
    def clean_patients_df(patients_df: DataFrame) -> DataFrame:
        """
        Clean the patients dataset by renaming columns, handling missing values,
        and correcting data types.

        Args:
            patients_df: Raw patients data as a PySpark DataFrame.
        
        Returns:
            Cleaned patients data as a PySpark DataFrame.
        """
        logger.info("Cleaning patients dataset...")

        # Rename columns for consistency
        column_mapping = {
            'PATIENT_ID': 'patient_id',
            'BIRTHDATE': 'birth_date',
            'DEATHDATE': 'death_date',
            'SSN': 'ssn',
            'DRIVERS': 'drivers_license',
            'PASSPORT': 'passport_number',
            'PREFIX': 'name_prefix',
            'FIRST': 'first_name',
            'LAST': 'last_name',
            'SUFFIX': 'name_suffix',
            'MAIDEN': 'maiden_name',
            'MARITAL': 'marital_status',
            'RACE': 'race',
            'ETHNICITY':'ethnicity',
            'GENDER': 'gender',
            'BIRTHPLACE': 'birthplace',
            'ADDRESS': 'address',
            'CITY': 'city',
            'STATE': 'state',
            'COUNTY': 'county',
            'FIPS': 'fips_code',
            'ZIP': 'zip_code',
            'LAT': 'latitude',
            'LON': 'longitude',
            'HEALTHCARE_EXPENSES': 'healthcare_expenses',
            'HEALTHCARE_COVERAGE': 'healthcare_coverage',
            'INCOME': 'annual_income'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in patients_df.columns:
                patients_df = patients_df.withColumnRenamed(old_col, new_col)

        # Step 1: Drop columns with excessive null values
        columns_to_drop = ['gender', 'name_suffix', 'death_date']
        columns_to_drop = [col for col in columns_to_drop if col in patients_df.columns]
        for column in columns_to_drop:
            patients_df = patients_df.drop(column)

        # Step 2: Replace null values in specific columns with 'unknown'
        columns_to_fill_unknown = ['drivers_license', 'passport_number', 'maiden_name', 'marital_status', 'name_prefix']
        for col_name in columns_to_fill_unknown:
            patients_df = patients_df.withColumn(
                col_name, when(col(col_name).isNull(), lit('unknown')).otherwise(col(col_name))
            )

        # Step 3: Standardize birth_date to a consistent datetime format
        patients_df = patients_df.withColumn("birth_date", to_date(col("birth_date")))

        # Step 4: Handle incorrect annual_income values
        income_median = patients_df.approxQuantile("annual_income", [0.5], 0)[0]
        patients_df = patients_df.withColumn(
            "annual_income", when(col("annual_income") < 0, lit(income_median)).otherwise(col("annual_income"))
        )

        # Drop any remaining null values
        patients_df = patients_df.na.drop()

        logger.info("Patients dataset cleaned successfully.")
        return patients_df
    

    @staticmethod
    def clean_medications_df(medications_df: DataFrame) -> DataFrame:
        """
        Clean the medications dataset.

        Args:
            medications_df: Raw medications data as a PySpark DataFrame.
        
        Returns:
            Cleaned medications data as a PySpark DataFrame.
        """
        logger.info("Cleaning medications dataset...")
        # Column renaming mapping
        column_mapping = {
            'START': 'start_date',
            'STOP': 'stop_date',
            'PATIENT': 'patient_id',
            'PAYER': 'payer',
            'ENCOUNTER': 'encounter_id',
            'CODE': 'medication_code',
            'DESCRIPTION': 'medication_description',
            'BASE_COST': 'base_cost',
            'PAYER_COVERAGE': 'payer_coverage',
            'DISPENSES': 'dispense_count',
            'TOTALCOST': 'total_cost',
            'REASONCODE': 'reason_code',
            'REASONDESCRIPTION': 'reason_description'
        }

        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in medications_df.columns:
                medications_df = medications_df.withColumnRenamed(old_col, new_col)

        # Lowercase patient_id and encounter_id
        medications_df = medications_df.withColumn('patient_id', lower(col('patient_id')))
        medications_df = medications_df.withColumn('encounter_id', lower(col('encounter_id')))

        # Convert start and stop dates to date type
        medications_df = medications_df.withColumn('start_date', to_date(col('start_date')))
        
        # Handle stop_date 
        medications_df = medications_df.withColumn(
            'stop_date', 
            when(col('stop_date').isNull(), lit(None))
            .otherwise(to_date(col('stop_date')))
        )

        # Handle null values in reason-related columns
        medications_df = medications_df.withColumn(
            'reason_code', 
            when(col('reason_code').isNull(), lit(-1))
            .otherwise(col('reason_code'))
        )

        medications_df = medications_df.withColumn(
            'reason_description', 
            when(col('reason_description').isNull(), lit('No Reason Specified'))
            .otherwise(col('reason_description'))
        )

        # Optional: Handle potential negative or zero values in numeric columns
        numeric_columns = ['base_cost', 'payer_coverage', 'dispense_count', 'total_cost']
        for numeric_col in numeric_columns:
            medications_df = medications_df.withColumn(
                numeric_col,
                when(col(numeric_col) < 0, lit(0)).otherwise(col(numeric_col))
            )

        # Drop rows with null patient_id or encounter_id
        medications_df = medications_df.na.drop()
        
        logger.info("Medications dataset cleaned successfully.")
        return medications_df



    @staticmethod
    def clean_symptoms_df(symptoms_df: DataFrame) -> DataFrame:
        """
        Clean the symptoms dataset.

        Args:
            symptoms_df: Raw symptoms data as a PySpark DataFrame.
        
        Returns:
            Cleaned symptoms data as a PySpark DataFrame.
        """
        logger.info("Cleaning symptoms dataset...")
        # Column renaming mapping
        column_mapping = {
            'PATIENT': 'patient_id',
            'RACE': 'race',
            'ETHNICITY': 'ethnicity',
            'AGE_BEGIN': 'age_begin',
            'PATHOLOGY': 'pathology',
            'NUM_SYMPTOMS': 'symptom_count',
            'SYMPTOMS': 'symptoms'
        }

        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in symptoms_df.columns:
                symptoms_df = symptoms_df.withColumnRenamed(old_col, new_col)
                
            # Step 1: Drop columns with excessive null values
        columns_to_drop = ['GENDER', 'AGE_END']
        
        # Only drop columns that exist
        columns_to_drop = [col for col in columns_to_drop if col in symptoms_df.columns]
        
        # Drop existing columns
        for column in columns_to_drop:
            symptoms_df = symptoms_df.drop(column)

        # Ensure symptom_count is non-negative
        symptoms_df = symptoms_df.withColumn(
            'symptom_count',
            when(col('symptom_count') < 0, lit(0)).otherwise(col('symptom_count'))
        )

        # Drop rows with null patient_id
        symptoms_df = symptoms_df.na.drop()
        logger.info("Symptoms dataset cleaned successfully.")
        return symptoms_df
    
    
    @staticmethod
    def load_conditions_to_csv(conditions: pd.DataFrame) -> pd.DataFrame:
        """Load conditions to csv because it's not possible to load excel directly into spark.
        """
        return conditions
    

    @staticmethod
    def clean_conditions_df(conditions_df: DataFrame) -> DataFrame:
        """
        Clean the conditions dataset.

        Args:
            conditions_df: Raw conditions data as a PySpark DataFrame.
        
        Returns:
            Cleaned conditions data as a PySpark DataFrame.
        """
        logger.info("Cleaning conditions dataset...")
        # Column renaming mapping
        column_mapping = {
            'START': 'start_date',
            'PATIENT': 'patient_id',
             'STOP'  : 'stop_date',
            'ENCOUNTER': 'encounter_id',
            'CODE': 'condition_code',
            'DESCRIPTION': 'condition_description'
        }

        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in conditions_df.columns:
                conditions_df = conditions_df.withColumnRenamed(old_col, new_col)

        # Remove STOP column (all NaN)
        if 'stop_date' in conditions_df.columns:
            conditions_df = conditions_df.drop('stop_date')

        # Lowercase patient and encounter IDs
        conditions_df = conditions_df.withColumn('patient_id', lower(col('patient_id')))
        conditions_df = conditions_df.withColumn('encounter_id', lower(col('encounter_id')))

        # Standardize condition description
        conditions_df = conditions_df.withColumn(
            'condition_description', 
            initcap(trim(lower(col('condition_description'))))
        )

        # Remove multiple spaces and trim
        conditions_df = conditions_df.withColumn(
            'condition_description', 
            regexp_replace(col('condition_description'), '\\s+', ' ')
        )

        conditions_df = conditions_df.withColumn("start_date", to_date(col("start_date")))
        # Drop rows with null patient_id
        conditions_df = conditions_df.na.drop()
        logger.info("Conditions dataset cleaned successfully.")
        return conditions_df


    @staticmethod
    def clean_encounters_df(encounters_df: DataFrame) -> DataFrame:
        """
        Clean the encounters dataset.

        Args:
            encounters_df: Raw encounters data as a PySpark DataFrame.
        
        Returns:
            Cleaned encounters data as a PySpark DataFrame.
        """
        logger.info("Cleaning encounters dataset...")
        # Column renaming mapping
        column_mapping = {
            'Id': 'encounter_id',
            'START': 'start_date',
            'STOP': 'stop_date',
            'PATIENT': 'patient_id',
            'REASONCODE':'reasoncode',
            'REASONDESCRIPTION':'reasondescription',
            'ORGANIZATION': 'organization',
            'PROVIDER': 'provider',
            'PAYER': 'payer',
            'ENCOUNTERCLASS': 'encounter_class',
            'CODE': 'encounter_code',
            'DESCRIPTION': 'encounter_description',
            'BASE_ENCOUNTER_COST': 'base_encounter_cost',
            'TOTAL_CLAIM_COST': 'total_claim_cost',
            'PAYER_COVERAGE': 'payer_coverage'
        }

        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in encounters_df.columns:
                encounters_df = encounters_df.withColumnRenamed(old_col, new_col)

        # Remove REASONCODE and REASONDESCRIPTION columns
        columns_to_drop = ['reasoncode', 'reasondescription']
        for column in columns_to_drop:
            if column in encounters_df.columns:
                encounters_df = encounters_df.drop(column)

        encounters_df = encounters_df.withColumn("start_date", to_date(col("start_date")))
        encounters_df = encounters_df.withColumn("stop_date", to_date(col("stop_date")))
        
        # Lowercase patient and encounter IDs
        encounters_df = encounters_df.withColumn('patient_id', lower(col('patient_id')))
        encounters_df = encounters_df.withColumn('encounter_id', lower(col('encounter_id')))

        # Ensure non-negative cost columns
        cost_columns = ['base_encounter_cost', 'total_claim_cost', 'payer_coverage']
        for cost_col in cost_columns:
            encounters_df = encounters_df.withColumn(
                cost_col, 
                when(col(cost_col) < 0, lit(0)).otherwise(col(cost_col))
            )

        # Drop rows with null patient_id or encounter_id
        encounters_df = encounters_df.na.drop(subset=['patient_id', 'encounter_id'])
        logger.info("Encounters dataset cleaned successfully.")
        encounters_df = encounters_df
        return encounters_df 
    

    @staticmethod
    def clean_patient_gender_df(patient_gender_df: DataFrame) -> DataFrame:
        """
        Clean the patient gender dataset.

        Args:
            patient_gender_df: Raw patient gender data as a PySpark DataFrame.
        
        Returns:
            Cleaned patient gender data as a PySpark DataFrame.
        """
        # Column renaming mapping
        column_mapping = {
            'Id': 'patient_id',
            'GENDER': 'gender'
        }
        
        logger.info("Cleaning patient gender dataset...")
        # Rename columns
        for old_col, new_col in column_mapping.items():
            if old_col in patient_gender_df.columns:
                patient_gender_df = patient_gender_df.withColumnRenamed(old_col, new_col)
                
        logger.info("Patient gender dataset cleaned successfully.")
        return patient_gender_df


import yaml
from pyspark.sql import DataFrame
from typing import Any, Dict

class save_to_database:
    @staticmethod
    def get_app_credentials() -> Dict[str, Any]:
        """
        Load credentials from the credentials.yml file.

        Returns:
            dict: Dictionary containing PostgreSQL credentials.
        """
        with open("./conf/base/credentials.yml") as cred:
            cred_dict = yaml.safe_load(cred).get("postgres_creds")
        return cred_dict


    @staticmethod
    def save_spark_to_postgres(
        spark_df: DataFrame,
        dataframe_name: str,
        mode: str = "overwrite",
        properties: Dict[str, Any] = None,
    ) -> None:
        """
        Save a Spark DataFrame to a PostgreSQL database using JDBC.
        This function automatically retrieves the database credentials.

        Args:
            spark_df (DataFrame): Spark DataFrame to be saved.
            mode (str, optional): Save mode. Defaults to "overwrite".
            properties (dict, optional): Additional connection properties.

        Raises:
            Exception: If there are issues saving the DataFrame.
        """
        # Load credentials using the get_app_credentials method
        creds = save_to_database.get_app_credentials()
        table_name = dataframe_name
        

        # Dynamically create the table name based on the DataFrame's name
        print(f"Generated table name: {table_name}")

        # Define default JDBC properties
        default_properties = {"driver": "org.postgresql.Driver"}
        connection_properties = {**default_properties, **(properties or {})}

        try:
            # Save DataFrame to PostgreSQL
            spark_df.write.format("jdbc") \
                .option("url", creds["url"]) \
                .option("dbtable", table_name) \
                .option("user", creds["user"]) \
                .option("password", creds["password"]) \
                .options(**{k: v for k, v in connection_properties.items() if k != "driver"}) \
                .option("driver", connection_properties["driver"]) \
                .mode(mode) \
                .save()

            print(f"Successfully saved DataFrame to table '{table_name}'")

        except Exception as e:
            print(f"Error saving DataFrame to PostgreSQL: {e}")
            raise




