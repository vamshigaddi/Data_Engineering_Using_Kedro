"""
This is a boilerplate pipeline 'data_cleaning'
generated using Kedro 0.19.10
"""

from kedro.pipeline import Pipeline, pipeline
from kedro.pipeline import Pipeline, node
from .nodes import DataCleaner,save_to_database

def create_pipeline(**kwargs) -> Pipeline:
    cleaner = DataCleaner()
    database = save_to_database()



    return Pipeline(
        [
            node(
                func=cleaner.load_conditions_to_csv,
                inputs="conditions_excel",
                outputs="conditions_csv",
                name="load_conditions_to_csv_node",
            ),
            node(
                func=cleaner.clean_patients_df,
                inputs="patients",
                outputs="cleaned_patients",
                name="clean_patients_data",
            ),
            node(
                func=cleaner.clean_medications_df,
                inputs="medications",
                outputs="cleaned_medications",
                name="clean_medications_data",
            ),
            node(
                func=cleaner.clean_symptoms_df,
                inputs="symptoms",
                outputs="cleaned_symptoms",
                name="clean_symptoms_data",
            ),
            node(
                func=cleaner.clean_conditions_df,
                inputs="conditions_spark",
                outputs="cleaned_conditions",
                name="clean_conditions_data",
            ),
            node(
                func=cleaner.clean_encounters_df,
                inputs="encounters",
                outputs="cleaned_encounters",
                name="clean_encounters_data",
            ),
            node(
                func=cleaner.clean_patient_gender_df,
                inputs="patients_gender",
                outputs="cleaned_patient_gender",
                name="clean_patient_gender_data",
            ),
            node(
                func=database.save_spark_to_postgres,
                inputs= ["cleaned_conditions","params:cleaned_conditions"],
                outputs=None,
                name="save_cleaned_conditions",
                
            ),
            node(
                func=database.save_spark_to_postgres,
                inputs= ["cleaned_patients","params:cleaned_patients"],
                outputs=None,
                name="save_cleaned_patients",  
                
            ),
            node(
                func=database.save_spark_to_postgres,
                inputs= ["cleaned_medications","params:cleaned_medications"],
                outputs=None,
                name="save_cleaned_medications",
                
            ),
            node(
                func=database.save_spark_to_postgres,
                inputs= ["cleaned_symptoms","params:cleaned_symptoms"],
                outputs=None,
                name="save_cleaned_cleaned_symptoms",
                
            ),
            node(
                func=database.save_spark_to_postgres,
                inputs= ["cleaned_encounters","params:cleaned_encounters"],
                outputs=None,
                name="save_cleaned_encounters",
                
            ),
            node(
                func=database.save_spark_to_postgres,
                inputs= ["cleaned_patient_gender","params:cleaned_patient_gender"],
                outputs=None,
                name="save_cleaned_patient_gender",
                
            ),
                        
        ]
    )
