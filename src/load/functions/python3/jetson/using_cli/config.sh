
dir_target=/data2/ar/workspace/efaas/src/Ilúvatar/target/release
port=6839
target_ip=localhost
target_ip="156.56.159.50"


compute=cpu
func_name=video_cpu
func_img=aarehman/video_processing-iluvatar-action:aarch64

compute=gpu
func_name=cnn_gpu
func_img=aarehman/cnn_image_classification_gpu-iluvatar-gpu:aarch64

