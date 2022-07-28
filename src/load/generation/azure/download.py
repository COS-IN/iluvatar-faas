import urllib.request as url
import argparse
import os
import tarfile

filename = "azurefunctions-dataset2019.tar.xz"

tar_url = "https://azurecloudpublicdataset2.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/{}".format(filename)

argparser = argparse.ArgumentParser()
argparser.add_argument("--out-folder", '-o')

args = argparser.parse_args()

target_file = os.path.join(args.out_folder, filename)

os.makedirs(args.out_folder, exist_ok=True)

print("downloading {} to {}".format(tar_url, target_file))

with open(target_file, "w+b") as f:
  with url.urlopen(tar_url) as download:
    f.write(download.read())

print("unzipping tar to ", args.out_folder)

with tarfile.open(target_file, "r:xz") as tar_f:
  tar_f.extractall(args.out_folder)

os.remove(target_file)
