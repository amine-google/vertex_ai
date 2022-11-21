from kfp.v2.dsl import (component, Input, Artifact, Model, Output, Metrics, ClassificationMetrics, HTML)
from typing import NamedTuple
                      
@component(base_image="tensorflow/tfx:1.5.1", packages_to_install=["model-card-toolkit", "seaborn", "pandas"])
def generate_model_card(
    gcp_project_id: str,
    input_table: Input[Artifact],
    metrics: Input[ClassificationMetrics],
    model_card_file: Output[HTML]
):
    
    from google.cloud import bigquery
    from datetime import date
    from io import BytesIO
    from IPython import display
    import model_card_toolkit as mctlib
    import base64
    import matplotlib.pyplot as plt
    import pandas as pd
    import seaborn as sns
    import uuid
    
    import os.path
    
    def plot_to_str():
        img = BytesIO()
        plt.savefig(img, format='png')
        return base64.encodebytes(img.getvalue()).decode('utf-8')
    

    # Loading input data
    project_id = input_table.metadata['projectId']
    dataset_id = input_table.metadata['datasetId']
    table_id = input_table.metadata['tableId']
   
    client = bigquery.Client(project = gcp_project_id)
    sql = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` where split != 'VALIDATE'"
    training_df = client.query(sql).to_dataframe()
    
    X_train = training_df[training_df['split']=='TRAIN']
    X_test = training_df[training_df['split']=='TEST']
    
    
    # Model Card Creation
    model_card_file.path = os.path.dirname(model_card_file.path) + "/model_cards/"
    mct = mctlib.ModelCardToolkit(model_card_file.path)
    model_card = mct.scaffold_assets()
    model_card.model_details.version.name = str(uuid.uuid4())
    model_card.model_details.version.date = str(date.today())
    
    # Model Card - Model General information 
    model_card.model_details.name = 'Churn Prediction Model'
    model_card.model_details.overview = ('The model predict which users will churn, based on the Order Items data')
    model_card.model_details.owners = [mctlib.Owner(name= 'Amine Hakkou', contact='aminehakkou@google.com')]
    model_card.model_details.references = [
        mctlib.Reference(reference='https://archive.ics.uci.edu/ml/datasets/Breast+Cancer+Wisconsin+(Diagnostic)'),
        mctlib.Reference(reference='https://minds.wisconsin.edu/bitstream/handle/1793/59692/TR1131.pdf')
    ]
    model_card.considerations.limitations = [mctlib.Limitation(description='Churn Analysis')]
    model_card.considerations.use_cases = [mctlib.UseCase(description='Churn Analysis')]
    model_card.considerations.users = [mctlib.User(description='Googlers'), mctlib.User(description='Curious People')]
    model_card.considerations.ethical_considerations = [
        mctlib.Risk(
            name=('Selection Bias'),
            mitigation_strategy='Automate the selection process'
        )
    ]
    
    # Model Card - Graphs
    
    sns.displot(x = X_train['nb_orders_last_7_days'], hue = X_train['is_churner'])
    dist_train_nb_orders_last_7_days = plot_to_str()

    sns.displot(x = X_train['nb_orders_last_15_days'], hue = X_train['is_churner'])
    dist_train_nb_orders_last_15_days = plot_to_str()
    
    model_card.model_parameters.data.append(mctlib.Dataset())
    model_card.model_parameters.data[0].graphics.description = (f'{len(X_train)} rows with {len(X_train.columns)} features')
    model_card.model_parameters.data[0].graphics.collection = [
        mctlib.Graphic(
            image = dist_train_nb_orders_last_7_days
        ),
        mctlib.Graphic(
            image = dist_train_nb_orders_last_15_days
        )
    ]
    
    
    
    sns.displot(x = X_test['nb_orders_last_7_days'], hue = X_test['is_churner'])
    dist_test_nb_orders_last_7_days = plot_to_str()

    sns.displot(x = X_test['nb_orders_last_15_days'], hue = X_test['is_churner'])
    dist_test_nb_orders_last_15_days = plot_to_str()
    
    model_card.model_parameters.data.append(mctlib.Dataset())
    model_card.model_parameters.data[1].graphics.description = (f'{len(X_test)} rows with {len(X_test.columns)} features')
    model_card.model_parameters.data[1].graphics.collection = [
        mctlib.Graphic(
            image = dist_test_nb_orders_last_7_days
        ),
        mctlib.Graphic(
            image = dist_test_nb_orders_last_15_days
        )
    ]
    
    # Create the Model Card
    mct.update_model_card(model_card)
    html = mct.export_format()

    

    