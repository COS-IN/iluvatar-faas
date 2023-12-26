msg = "good"
import traceback
try:
    # import boto3
    import urllib.request
    import uuid
    from time import time
    import cv2
    cv2.setNumThreads(1)
    # s3_client = boto3.client('s3')
except Exception as e:
    msg = traceback.format_exc()

tmp = "/tmp/"
FILE_NAME_INDEX = 0
FILE_PATH_INDEX = 2


def video_processing(object_key, video_path):
    file_name = object_key.split(".")[FILE_NAME_INDEX]
    result_file_path = tmp+file_name+'-output.avi'

    video = cv2.VideoCapture(video_path)

    width = int(video.get(3))
    height = int(video.get(4))

    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    out = cv2.VideoWriter(result_file_path, fourcc, 20.0, (width, height))

    while video.isOpened():
        ret, frame = video.read()

        if ret:
            gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            tmp_file_path = tmp+'tmp.jpg'
            cv2.imwrite(tmp_file_path, gray_frame)
            gray_frame = cv2.imread(tmp_file_path)
            out.write(gray_frame)
        else:
            break

    video.release()
    out.release()
    return result_file_path

cold = True

def main(args):
    global cold
    was_cold = cold
    cold = False
    # return {"body": { "msg":"TEST", "latency":0, "cold":was_cold }}
    try:
        start = time()

        input_bucket = args.get("input_bucket", "")
        object_key = args.get("object_key", "")
        output_bucket = args.get("output_bucket", "")

        download_path = tmp+'{}{}'.format(uuid.uuid4(), object_key)

        # s3_client.download_file(input_bucket, object_key, download_path)
        src = "https://sample-videos.com/video123/mp4/720/big_buck_bunny_720p_1mb.mp4"
        urllib.request.urlretrieve(src, download_path)

        upload_path = video_processing(object_key, download_path)

        # TODO: Upload
        # s3_client.upload_file(upload_path, output_bucket, upload_path.split("/")[FILE_PATH_INDEX])
        end = time()
        latency = end - start

        return {"body": { "latency":latency, "cold":was_cold, "start":start, "end":end }}
    except Exception as e:
        return {"body": { "cust_error":traceback.format_exc(), "msg":msg, "cold":was_cold }}


if __name__ == '__main__':
    r = main({})
    print(r)
