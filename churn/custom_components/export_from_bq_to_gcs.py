from kfp.v2.dsl import (component, Input, Output, Artifact, Dataset)
                        
@component(base_image="python:3.9", packages_to_install=["google-cloud-aiplatform"])
def export_from_bq_to_gcs(
    gcp_project_id: str,
    bq_job_output: Input[Artifact], 
    gcs_dataset: Output[Dataset]
):
    
    from google.cloud import bigquery
    from google.cloud import storage
    
    #storage_client = storage.Client(project = gcp_project_id)
    #print(gcs_dataset.path)
    #storage_client.create_bucket(gcs_dataset.path.replace("gs://",""))

    # Loading input data
    project_id = bq_job_output.metadata['projectId']
    dataset_id = bq_job_output.metadata['datasetId']
    table_id = bq_job_output.metadata['tableId']
    
    # Output bucket
    gcs_dataset.path = gcs_dataset.path + "/"
   
    client = bigquery.Client(project = gcp_project_id)
    sql = f"""
        EXPORT DATA
        OPTIONS (
            uri = 'gs:/{gcs_dataset.path.replace("gcs/", "")}*.csv',
            format = 'CSV',
            overwrite = true,
            header = true,
            field_delimiter = ';'
        )
        AS (
            SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
        );
    """
    
    print(sql)
    
    client.query(sql)

