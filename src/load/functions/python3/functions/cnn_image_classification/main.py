msg = "good"
import traceback
try:
    import numpy as np
    import uuid
    from time import time

    # import boto3
    import tensorflow as tf
    from tensorflow.keras.preprocessing import image
    from tensorflow.keras.applications.resnet50 import preprocess_input, decode_predictions
    from tensorflow.keras.utils import get_file
    from squeezenet import SqueezeNet

    # s3_client = boto3.client('s3')

except Exception as e:
    msg = traceback.format_exc()

tmp = "/tmp/"
cold = True

def predict(img_local_path):
    start = time()
    config = tf.ConfigProto(device_count={"CPU": 1}, 
                inter_op_parallelism_threads = 1, 
                intra_op_parallelism_threads = 1,
                log_device_placement=True)

    with tf.Session(config=config) as sess:
      model = SqueezeNet(weights='imagenet')
      img = image.load_img(img_local_path, target_size=(227, 227))
      x = image.img_to_array(img)
      x = np.expand_dims(x, axis=0)
      x = preprocess_input(x)
      preds = model.predict(x)
      res = decode_predictions(preds)
    end = time()
    latency = end - start
    return latency, res, start, end


def main(args):
    global cold
    was_cold = cold
    cold = False
    # return {"body": { "latency":-1, "msg":msg, "cold":was_cold }}
    try:
        input_bucket = args.get("input_bucket", 1)
        object_key = args.get("object_key", 1)

        model_object_key = args.get("model_object_key", 1) # example : squeezenet_weights_tf_dim_ordering_tf_kernels.h5
        model_bucket = args.get("model_bucket", 1)

        # download_path = tmp + '{}{}'.format(uuid.uuid4(), object_key)
        download_path = get_file('{}{}'.format(uuid.uuid4(), object_key),
                                    "https://github.com/kmu-bigdata/serverless-faas-workbench/raw/master/dataset/image/animal-dog.jpg",
                                    cache_dir='/tmp/')
        # s3_client.download_file(input_bucket, object_key, download_path)

        model_path = tmp + '{}{}'.format(uuid.uuid4(), model_object_key)
        # s3_client.download_file(model_bucket, model_object_key, model_path)
            
        latency, result, start, end = predict(download_path)
            
        _tmp_dic = {x[1]: {'N': str(x[2])} for x in result[0]}

        return {"body": { "latency":latency, "msg":msg, "cold":was_cold, "start":start, "end":end }}
    except Exception as e:
        err = "whelp"
        try:
            err = traceback.format_exc()
        except Exception as fug:
            err = str(fug)
        return {"body": { "cust_error":msg, "thing":err, "cold":was_cold }}