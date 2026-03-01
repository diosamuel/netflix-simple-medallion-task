import boto3 
import pandas as pd
import io


s3 = boto3.client('s3',
    endpoint_url = 'http://minio:9000',
    aws_access_key_id = 'admin',
    aws_secret_access_key = 'admin123'
)

res = s3.get_object(Bucket='bronze',Key='netflix_titles.csv')
text = io.BytesIO(res['Body'].read())

df = pd.read_csv(text)
df['explode_list'] = df['listed_in'].str.split(',').apply(lambda x:list(map(lambda y:y.strip(),x)))
df = df.explode('explode_list')
df['processed_at'] = datetime.now()

# to silver
output=io.BytesIO()
df.to_parquet(output,engine='pyarrow',index=False)
output.seek(0)

s3.put_object(
    Bucket='silver',
    Key='silver.parquet',
    Body=output.getvalue()
)