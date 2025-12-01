Steps to deploy :

C:\Tools\databricks\databricks.exe bundle validate -t dev
C:\Tools\databricks\databricks.exe bundle deploy -t dev
C:\Tools\databricks\databricks.exe bundle run batch_inference_job -t dev