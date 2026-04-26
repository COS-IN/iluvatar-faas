#!/bin/bash

echo "Running benchmark"
source ../examples-venv/bin/activate

help () {
cat << EOF
Run the benchmark example using prebuilt images or code uploads

[--code]: Run the code upload example.
[--images]: Run the prebuilt iomages upload example.
[--help]: Display this help information.
EOF
exit 1
}

CODE=""
IMAGES=""
for i in "$@"
do
case $i in
    --code)
    CODE=true
    ;;
    --images)
    IMAGES=true
    ;;
    -h|--help)
    help
    ;;
    *)
    # unknown option
    help
    ;;
esac
done

if [[ -z "$CODE" && -z "$IMAGES" ]]; then
help
fi

if [[ "$CODE" -eq "true" ]]; then
    python3 bench-code-upload.py
fi

if [[ "$IMAGES" -eq "true" ]]; then
    python3 bench-images.py
fi

deactivate