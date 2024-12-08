"""
This is a boilerplate pipeline 'combined_data'
generated using Kedro 0.19.10
"""

from kedro.pipeline import Pipeline, pipeline


from kedro.pipeline import Pipeline, node
from .nodes import combine_healthcare_data

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=combine_healthcare_data,
                inputs=[
                    "cleaned_medications",
                    "cleaned_conditions",
                    "cleaned_patient_gender",
                    "cleaned_patients",
                    "cleaned_symptoms",
                    "cleaned_encounters",
                ],
                outputs="model_input_table",
                name="combine_healthcare_data_node",
            ),
        ]
    )
