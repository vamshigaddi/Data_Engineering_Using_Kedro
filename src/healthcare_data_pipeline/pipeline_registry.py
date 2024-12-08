"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline


# def register_pipelines() -> dict[str, Pipeline]:
#     """Register the project's pipelines.

#     Returns:
#         A mapping from pipeline names to ``Pipeline`` objects.
#     """
#     pipelines = find_pipelines()
#     pipelines["__default__"] = sum(pipelines.values())
#     return pipelines


from .pipelines.data_cleaning import create_pipeline as cleaning_pipeline
from.pipelines.combined_data import create_pipeline as combine_pipeline
def register_pipelines() -> dict[str, Pipeline]:
    return {
        "__default__": cleaning_pipeline() + combine_pipeline(),
        "cleaning": cleaning_pipeline(),
        "combining":combine_pipeline(),
    }
