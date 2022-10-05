from kfp.v2.dsl import (component, Input, Artifact)
                        
@component(base_image="python:3.9", packages_to_install=["google-cloud-aiplatform"])
def get_bq_job_output_table(bq_job_output: Input[Artifact]) -> str:
   project_id = bq_job_output.metadata['projectId']
   dataset_id = bq_job_output.metadata['datasetId']
   table_id = bq_job_output.metadata['tableId']
   return f"bq://{project_id}.{dataset_id}.{table_id}"