#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
IMAGE_PREFIX=${IMAGE_PREFIX:-aarehman/}
IMAGE_TAG=${IMAGE_TAG:-latest}

docker_cmd=(docker)
if ! docker info >/dev/null 2>&1; then
	docker_cmd=(sudo docker)
fi

mapfile -t source_images < <(
	"${docker_cmd[@]}" image ls --format '{{.Repository}}:{{.Tag}}' |
		awk -v prefix="$IMAGE_PREFIX" -v image_tag="$IMAGE_TAG" \
			'index($0, prefix) == 1 && $0 ~ (":" image_tag "$")' |
		sort -u
)

if ((${#source_images[@]} == 0)); then
	echo "No local ${IMAGE_PREFIX} images found." >&2
	exit 1
fi

for source_image in "${source_images[@]}"; do
	repository=${source_image%:*}
	target_image="${repository}:${IMAGE_TAG}"
	echo "Replacing ${target_image} with its marker-enabled variant"
	"${docker_cmd[@]}" build \
		--build-arg "BASE_IMAGE=${source_image}" \
		--tag "$target_image" \
		"$SCRIPT_DIR"
done

echo "Built ${#source_images[@]} marker-enabled images."
