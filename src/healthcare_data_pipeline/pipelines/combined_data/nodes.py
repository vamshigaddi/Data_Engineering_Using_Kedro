"""
This is a boilerplate pipeline 'combined_data'
generated using Kedro 0.19.10
"""
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def combine_healthcare_data(
    df_medications: DataFrame,
    df_conditions: DataFrame,
    df_patient_gender: DataFrame,
    df_patients: DataFrame,
    df_symptoms: DataFrame,
    df_encounter: DataFrame
) -> DataFrame:
    """
    Combine multiple healthcare DataFrames into one unified DataFrame using patient_id as the key.

    Parameters:
    ----------
    df_medications : DataFrame
        The medications DataFrame.
    df_conditions : DataFrame
        The conditions DataFrame.
    df_patient_gender : DataFrame
        The patient gender DataFrame.
    df_patients : DataFrame
        The patient details DataFrame.
    df_symptoms : DataFrame
        The symptoms DataFrame.
    df_encounter : DataFrame
        The encounter DataFrame.

    Returns:
    -------
    DataFrame
        A unified DataFrame containing merged data from all input DataFrames.
    """
    # Merge medications with conditions
    merged_df = df_medications.join(
        df_conditions, on=['patient_id', 'encounter_id', 'start_date'], how='outer'
    )

    # Merge with patient gender
    merged_df = merged_df.join(
        df_patient_gender, on='patient_id', how='outer'
    )

    # Merge with patient details
    merged_df = merged_df.join(
        df_patients, on='patient_id',how='outer'
    )

    # Merge with symptoms
    merged_df = merged_df.join(
        df_symptoms, on=['patient_id','race','ethnicity'],how='outer'
    )

    # Merge with encounters
    merged_df = df_encounter.join(
        merged_df, on=['patient_id','start_date','stop_date','encounter_id','payer','payer_coverage'],how='outer'
    )

    return merged_df
