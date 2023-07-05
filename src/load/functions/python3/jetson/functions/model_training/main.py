# import boto3
msg = "good"
# import re
import traceback
try:
  import os, sys
  os.environ['MKL_NUM_THREADS'] = '1'
  os.environ['OPENBLAS_NUM_THREADS'] = '1'

  from sklearn.feature_extraction.text import TfidfVectorizer
  from sklearn.linear_model import LogisticRegression
  import joblib

  import urllib.request
  import pandas as pd
  from time import time
  import uuid
  import re
  import io
except Exception as e:
    msg = traceback.format_exc()
# s3_client = boto3.client('s3')

cleanup_re = re.compile('[^a-z]+')
tmp = '/tmp/'


def cleanup(sentence):
    sentence = sentence.lower()
    sentence = cleanup_re.sub(' ', sentence).strip()
    return sentence

cold = True

def main(args):
    global cold
    was_cold = cold
    cold = False
    # return {"body": { "latency":-1, "cold":was_cold, "msg":msg }}
    try:
        start = time()
        dataset_bucket = args.get("dataset_bucket", "")
        dataset_object_key = args.get("dataset_object_key", "")
        model_bucket = args.get("model_bucket", "")
        model_object_key = args.get("model_object_key", "test.mdl")

        # obj = s3_client.get_object(Bucket=dataset_bucket, Key=dataset_object_key)
        opts = [10, 20, 50, 100]
        download_path = tmp+'{}{}'.format(uuid.uuid4(), dataset_object_key)
        src = "https://github.com/kmu-bigdata/serverless-faas-workbench/raw/master/dataset/amzn_fine_food_reviews/reviews20mb.csv"
        urllib.request.urlretrieve(src, download_path)
        df = pd.read_csv(download_path)

        df['train'] = df['Text'].apply(cleanup)

        tfidf_vector = TfidfVectorizer(min_df=100).fit(df['train'])

        train = tfidf_vector.transform(df['train'])

        model = LogisticRegression(n_jobs=1)
        model.fit(train, df['Score'])

        model_file_path = tmp + model_object_key
        joblib.dump(model, model_file_path)

        end = time()
        latency = end - start

        # TODO: Upload
        # s3_client.upload_file(model_file_path, model_bucket, model_object_key)

        return {"body": { "latency":latency, "cold":was_cold, "start":start, "end":end }}
    except Exception as e:
        return {"body": { "cust_error":traceback.format_exc(), "msg":msg, "cold":was_cold }}