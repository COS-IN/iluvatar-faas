msg = "good"
import traceback
try:
  import numpy as np
  import tensorflow as tf
  import os.path
  import tarfile
  import re
  import urllib.request, urllib.parse, urllib.error
  from time import time
  import logging
  # tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.WARNING)
  tf.get_logger().setLevel('WARNING')
  from tensorflow.compat.v1 import ConfigProto
  from tensorflow.compat.v1 import InteractiveSession
  config = ConfigProto()
  config.gpu_options.allow_growth = True
  config.gpu_options.per_process_gpu_memory_fraction = 0.15
  session = InteractiveSession(config=config)
  
  tf.keras.utils.disable_interactive_logging()
except Exception as e:
  msg = traceback.format_exc()

"""
Originally https://github.com/ryfeus/lambda-packs
"""

def downloadFromURL(url, local_path):
  print("downloadFromURL")
  if not os.path.exists(local_path):
    urllib.request.urlretrieve(url, local_path)

def create_graph():
    print("create_graph")
    with tf.compat.v1.gfile.FastGFile(os.path.join('/tmp/imagenet/', 'classify_image_graph_def.pb'), 'rb') as f:
        graph_def = tf.compat.v1.GraphDef()
        graph_def.ParseFromString(f.read())
        print("import_graph_def")
        _ = tf.import_graph_def(graph_def, name='')
        print("import_graph_def done")

class NodeLookup(object):
    """Converts integer node ID's to human readable labels."""

    def __init__(self,
               label_lookup_path=None,
               uid_lookup_path=None):
        print("NodeLookup __init__")
        if not label_lookup_path:
            label_lookup_path = os.path.join(
                '/tmp/imagenet/', 'imagenet_2012_challenge_label_map_proto.pbtxt')
        if not uid_lookup_path:
            uid_lookup_path = os.path.join(
                '/tmp/imagenet/', 'imagenet_synset_to_human_label_map.txt')
        self.node_lookup = self.load(label_lookup_path, uid_lookup_path)

    def load(self, label_lookup_path, uid_lookup_path):
        if not tf.io.gfile.exists(uid_lookup_path):
            tf.logging.fatal('File does not exist %s', uid_lookup_path)
        if not tf.io.gfile.exists(label_lookup_path):
            tf.logging.fatal('File does not exist %s', label_lookup_path)

        # Loads mapping from string UID to human-readable string
        proto_as_ascii_lines = tf.io.gfile.GFile(uid_lookup_path).readlines()
        uid_to_human = {}
        p = re.compile(r'[n\d]*[ \S,]*')
        for line in proto_as_ascii_lines:
            parsed_items = p.findall(line)
            uid = parsed_items[0]
            human_string = parsed_items[2]
            uid_to_human[uid] = human_string

        # Loads mapping from string UID to integer node ID.
        node_id_to_uid = {}
        proto_as_ascii = tf.io.gfile.GFile(label_lookup_path).readlines()
        for line in proto_as_ascii:
            if line.startswith('  target_class:'):
                target_class = int(line.split(': ')[1])
            if line.startswith('  target_class_string:'):
                target_class_string = line.split(': ')[1]
                node_id_to_uid[target_class] = target_class_string[1:-2]

        # Loads the final mapping of integer node ID to human-readable string
        node_id_to_name = {}
        for key, val in list(node_id_to_uid.items()):
            if val not in uid_to_human:
                tf.logging.fatal('Failed to locate: %s', val)
            name = uid_to_human[val]
            node_id_to_name[key] = name

        return node_id_to_name

    def id_to_string(self, node_id):
        if node_id not in self.node_lookup:
            return ''
        return self.node_lookup[node_id]

def run_inference_on_image(image):
    print("run_inference_on_image")
    if not tf.io.gfile.exists(image):
        tf.logging.fatal('File does not exist %s', image)
    image_data = tf.compat.v1.gfile.FastGFile(image, 'rb').read()

    with tf.compat.v1.Session() as sess:
        # Some useful tensors:
        # 'softmax:0': A tensor containing the normalized prediction across
        #   1000 labels.
        # 'pool_3:0': A tensor containing the next-to-last layer containing 2048
        #   float description of the image.
        # 'DecodeJpeg/contents:0': A tensor containing a string providing JPEG
        #   encoding of the image.
        # Runs the softmax tensor by feeding the image_data as input to the graph.
        softmax_tensor = sess.graph.get_tensor_by_name('softmax:0')
        predictions = sess.run(softmax_tensor,
                               {'DecodeJpeg/contents:0': image_data})
        predictions = np.squeeze(predictions)

        # Creates node ID --> English string lookup.
        node_lookup = NodeLookup()

        top_k = predictions.argsort()[-5:][::-1]
        strResult = '%s (score = %.5f)' % (node_lookup.id_to_string(top_k[0]), predictions[top_k[0]])
        for node_id in top_k:
            human_string = node_lookup.id_to_string(node_id)
            score = predictions[node_id]
            print('%s (score = %.5f)' % (human_string, score))
    return strResult

cold = True

def main(args):
  global cold
  was_cold = cold
  cold = False
  if not os.path.exists('/tmp/imagenet/'):
      os.makedirs('/tmp/imagenet/')

  downloadFromURL(
      url='http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz',
      local_path='/tmp/imagenet/inception-2015-12-05.tgz'
  )

  if not os.path.exists("/tmp/imagenet/classify_image_graph_def.pb"):
      file = tarfile.open('/tmp/imagenet/inception-2015-12-05.tgz', 'r:gz')
      file.extractall('/tmp/imagenet/')
  create_graph()

  try:
    start = time()

    if 'imagelink' in args:
      urllib.request.urlretrieve(args['imagelink'], '/tmp/imagenet/inputimage.jpg')

    if os.path.exists(os.path.join('/tmp/imagenet/', 'inputimage.jpg')):
        image = os.path.join('/tmp/imagenet/', 'inputimage.jpg')
    else: 
        image = '/tmp/imagenet/cropped_panda.jpg'
    inter_start = time()
    # strResult = "TEST"
    strResult = run_inference_on_image(image)
    end = time()
    return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end, "output":strResult, "infer_latency":end-inter_start }}
  except Exception as e:
    err = str(e)
    try:
        err = traceback.format_exc()
    except Exception as fug:
        err = str(fug)
    return {"body": { "import_error":msg, "runtime_error":err, "cold":was_cold }}