from kfp.v2.dsl import (component, Input, Artifact, Model, Output)
                      
@component(base_image="python:3.9", packages_to_install=["google-cloud-aiplatform", "scikit-learn", "pandas", "pyarrow"])
def sklearn_validator(
    input_model: Input[Model],
    input_table: Input[Artifact],
    gcp_project_id: str
):
    from sklearn.ensemble import RandomForestClassifier
    import pandas as pd
    import logging 
    import joblib
    from sklearn.metrics import roc_curve, confusion_matrix, accuracy_score
    import json
    import typing

    
    project_id = input_table.metadata['projectId']
    dataset_id = input_table.metadata['datasetId']
    table_id = input_table.metadata['tableId']
   
    client = bigquery.Client(project = gcp_project_id)
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` where split = 'VALIDATE'"
    validation_df = client.query(sql).to_dataframe()

    