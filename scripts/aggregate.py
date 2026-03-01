import pandas as pd
import io
import boto3

s3 = boto3.client('s3',
    endpoint_url = 'http://minio:9000',
    aws_access_key_id = 'admin',
    aws_secret_access_key = 'admin123'
)

res = s3.get_object(Bucket='silver',Key='silver.parquet')
data = res['Body'].read()
gold = pd.read_parquet(io.BytesIO(data), engine="pyarrow")

# aggregate dengan menghitung total kategori pada dataset netflix
df = pd.DataFrame(gold['explode_list'].value_counts()).reset_index()

# simpan ke file parquet
output=io.BytesIO()
df.to_parquet(output,engine='pyarrow',index=False)
output.seek(0)

s3.put_object(
    Bucket='gold',
    Key='gold.parquet',
    Body=output.getvalue()
)

print(df.rename(columns={'explode_list':'kategori'}))