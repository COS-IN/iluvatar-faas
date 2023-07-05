msg = "good"
import traceback
try:
    # import boto3
    import urllib.request
    import uuid
    from time import time
    from PIL import Image, ImageFilter
except Exception as e:
    msg = traceback.format_exc()
# s3_client = boto3.client('s3')
FILE_NAME_INDEX = 2

TMP = "/tmp/"

def flip(image, file_name):
    path_list = []
    path = TMP + "flip-left-right-" + file_name
    img = image.transpose(Image.FLIP_LEFT_RIGHT)
    img.save(path)
    path_list.append(path)

    path = TMP + "flip-top-bottom-" + file_name
    img = image.transpose(Image.FLIP_TOP_BOTTOM)
    img.save(path)
    path_list.append(path)

    return path_list

def rotate(image, file_name):
    path_list = []
    path = TMP + "rotate-90-" + file_name
    img = image.transpose(Image.ROTATE_90)
    img.save(path)
    path_list.append(path)

    path = TMP + "rotate-180-" + file_name
    img = image.transpose(Image.ROTATE_180)
    img.save(path)
    path_list.append(path)

    path = TMP + "rotate-270-" + file_name
    img = image.transpose(Image.ROTATE_270)
    img.save(path)
    path_list.append(path)

    return path_list

def filter(image, file_name):
    path_list = []
    path = TMP + "blur-" + file_name
    img = image.filter(ImageFilter.BLUR)
    img.save(path)
    path_list.append(path)

    path = TMP + "contour-" + file_name
    img = image.filter(ImageFilter.CONTOUR)
    img.save(path)
    path_list.append(path)

    path = TMP + "sharpen-" + file_name
    img = image.filter(ImageFilter.SHARPEN)
    img.save(path)
    path_list.append(path)

    return path_list

def gray_scale(image, file_name):
    path = TMP + "gray-scale-" + file_name
    img = image.convert('L')
    img.save(path)
    return [path]

def resize(image, file_name):
    path = TMP + "resized-" + file_name
    image.thumbnail((128, 128))
    image.save(path)
    return [path]

def image_processing(file_name, image_path):
    path_list = []
    start = time()
    with Image.open(image_path) as image:
        tmp = image
        path_list += flip(image, file_name)
        path_list += rotate(image, file_name)
        path_list += filter(image, file_name)
        path_list += gray_scale(image, file_name)
        path_list += resize(image, file_name)

    latency = time() - start
    return latency, path_list

cold = True

def main(args):
    start = time()
    global cold
    was_cold = cold
    cold = False
    try:
        input_bucket = args.get("input_bucket", "test")
        object_key = args.get("object_key", "file.jpg")
        output_bucket = args.get("output_bucket", "test")

        download_path = '/tmp/{}{}'.format(uuid.uuid4(), object_key)

        # s3_client.download_file(input_bucket, object_key, download_path)
        src = "https://github.com/kmu-bigdata/serverless-faas-workbench/raw/master/dataset/image/image.jpg"
        urllib.request.urlretrieve(src, download_path)

        latency, path_list = image_processing(object_key, download_path)

        # TODO: Upload
        # for upload_path in path_list:
        #     s3_client.upload_file(upload_path, output_bucket, upload_path.split("/")[FILE_NAME_INDEX])

        end = time()
        return {"body": { "latency":end-start, "cold":was_cold, "start":start, "end":end }}
    except Exception as e:
        return {"body": { "cust_error":traceback.format_exc(), "msg":msg, "cold":was_cold }}