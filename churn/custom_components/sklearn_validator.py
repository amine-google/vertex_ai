from kfp.v2.dsl import (component, Input, Artifact, Model, Output, Metrics, ClassificationMetrics)
from typing import NamedTuple
                      
@component(base_image="python:3.9", packages_to_install=["google-cloud-aiplatform", "scikit-learn", "pandas", "pyarrow", "dill"])
def sklearn_validator(
    input_model: Input[Model],
    input_table: Input[Artifact],
    gcp_project_id: str,
    thresholds_dict_str: str,
    metrics: Output[ClassificationMetrics],
    kpi: Output[Metrics]
) -> NamedTuple("output", [("deploy", str)]):
    
    from google.cloud import bigquery
    from sklearn.ensemble import RandomForestClassifier
    import pandas as pd
    import logging 
    import dill as pickle
    from sklearn.metrics import roc_curve, confusion_matrix, accuracy_score
    import json
    import typing
    
    def threshold_check(val1, val2):
        cond = "false"
        if val1 >= val2 :
            cond = "true"
        return cond
    

    # Loading validation data
    project_id = input_table.metadata['projectId']
    dataset_id = input_table.metadata['datasetId']
    table_id = input_table.metadata['tableId']
   
    client = bigquery.Client(project = gcp_project_id)
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` where split = 'VALIDATE'"
    validation_df = client.query(sql).to_dataframe()
    
    
    # Loading the input model
    #file_name = input_model.path + ".pkl"
    with open(input_model.path, 'rb') as file:  
        model = pickle.load(file)
        
    
    # Computing predictions
    X_val, Y_val = validation_df.drop(['is_churner'], axis=1), validation_df['is_churner']
    Y_pred = model.predict(X_val)
    Y_scores =  model.predict_proba(X_val)[:, 1]
    
    
    # Computing ROC curve
    fpr, tpr, thresholds = roc_curve(
        y_true = Y_val, 
        y_score = Y_scores, 
        pos_label = True
    )
    metrics.log_roc_curve(fpr.tolist(), tpr.tolist(), thresholds.tolist()) 
    
    
    # Computing confusion matrix
    metrics.log_confusion_matrix(
       ["False", "True"],
       confusion_matrix(Y_val, Y_pred).tolist(), 
    )
    
    
    # Computing other metrics
    accuracy = accuracy_score(Y_val, Y_pred.round())
    thresholds_dict = json.loads(thresholds_dict_str)
    input_model.metadata["accuracy"] = float(accuracy)
    kpi.log_metric("accuracy", float(accuracy))
    deploy = threshold_check(float(accuracy), int(thresholds_dict['roc']))
    
    return (deploy,)

    