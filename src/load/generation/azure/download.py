import urllib.request as url
import argparse
import os
import tarfile


def download(target_folder):
    filename = "azurefunctions-dataset2019.tar.xz"
    tar_url = "https://azurepublicdatasettraces.blob.core.windows.net/azurepublicdatasetv2/azurefunctions_dataset2019/{}".format(
        filename
    )

    target_file = os.path.join(target_folder, filename)
    os.makedirs(target_folder, exist_ok=True)
    print("downloading {} to {}".format(tar_url, target_file))

    with open(target_file, "w+b") as f:
        with url.urlopen(tar_url) as download:
            f.write(download.read())

    print("unzipping tar to ", target_folder)
    with tarfile.open(target_file, "r:xz") as tar_f:
        tar_f.extractall(target_folder)

    os.remove(target_file)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--out-folder", "-o", required=True)
    args = argparser.parse_args()
    download(args.out_folder)
