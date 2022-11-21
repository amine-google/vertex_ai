from kfp.v2.dsl import (component, Input, Artifact, Model, Output)
                      
@component(base_image="python:3.9", packages_to_install=["google-cloud-aiplatform", "scikit-learn", "pandas", "pyarrow", "dill"])
def sklearn_trainer(
    input_table: Input[Artifact],
    gcp_project_id: str,
    model: Output[Model]
):
    from google.cloud import bigquery
    from sklearn.preprocessing import StandardScaler
    from sklearn.feature_selection import SelectKBest
    from sklearn.feature_selection import chi2
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import RandomizedSearchCV
    from sklearn.model_selection import PredefinedSplit
    from sklearn.pipeline import Pipeline
    import pandas
    import numpy as np
    import dill as pickle
        
    class columnDropperTransformer():
        def __init__(self,columns):
            self.columns=columns
        def transform(self,X,y=None):
            return X.drop(self.columns,axis=1)
        def fit(self, X, y=None):
            return self 
    
    project_id = input_table.metadata['projectId']
    dataset_id = input_table.metadata['datasetId']
    table_id = input_table.metadata['tableId']
   
    client = bigquery.Client(project = gcp_project_id)
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` where split != 'VALIDATE'"
    training_df = client.query(sql).to_dataframe()

    model_pipeline = Pipeline(
        [
            ("columnDropper", columnDropperTransformer(['user_id', 'bucket', 'split'])),
            ('scaler', StandardScaler()),
            (
                'classification', 
                RandomizedSearchCV(
                    RandomForestClassifier(),
                    param_distributions = {
                        "n_estimators": np.arange(1, 2, 3),
                        "max_depth": [3, 5]
                    },
                    n_iter = 2,
                    refit = True,
                    cv = PredefinedSplit(test_fold = training_df[training_df['split']=='TEST'].index) # Predefined split (no cross validation)
                )
            )
        ]
    )

    X, Y = training_df.drop(['is_churner'], axis=1), training_df['is_churner']

    model_pipeline.fit(X, Y)
    
    # Uploading the model to PIPELINE_ROOT
    model.metadata["framework"] = "RF"
    
    model.path = model.path + f".pkl"
    with open(model.path, 'wb') as file:  
        pickle.dump(model_pipeline, file)

