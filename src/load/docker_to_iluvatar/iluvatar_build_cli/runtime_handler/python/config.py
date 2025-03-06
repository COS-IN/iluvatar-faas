# Python runtime configuration.
BASE_IMAGE = "docker.io/sakbhatt/iluvatar_base_image"
BASE_IMAGE_GPU = "docker.io/sakbhatt/iluvatar_base_image_gpu"
SERVER_COMMAND = "gunicorn -w 1 server:app"
