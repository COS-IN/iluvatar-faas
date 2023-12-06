#!/bin/bash

# combines the energy monitor with lookbusy to get a siple test of usage and a baseline value
# to run:
# ./simple-energy.sh -i=127.0.0.200 --ipmi-pass-f=~/ipmipass

OUT_DIR="."
for i in "$@"
do
case $i in
    -i=*|--ipadddr=*)
    IPMI_ADDR="${i#*=}"
    ;;
    --ipmi-pass-f=*)
    IPMI_PASS="${i#*=}"
    ;;
    --out-dir=*)
    OUT_DIR="${i#*=}"
    ;;
    *)
      # unknown option
    ;;
esac
done

MONITOR_MS=1000

if [ ! -f ./lookbusy-1.4/lookbusy ]
then
# set up lookbusy if missing
wget http://www.devin.com/lookbusy/download/lookbusy-1.4.tar.gz && tar -xvf lookbusy-1.4.tar.gz \
  && rm lookbusy-1.4.tar.gz && cd ./lookbusy-1.4 && ./configure && make && cd ..
fi

##################################################################################################################################
# CPU 
sub_dir="$OUT_DIR/cpu"
mkdir -p $sub_dir
# waiting for IPMI to return to normal
sleep 5s

../../../Ilúvatar/target/release/iluvatar_energy_mon --enable-rapl --enable-ipmi --enable-perf --log-folder $sub_dir \
--log-freq-ms $MONITOR_MS --perf-stat-duration-ms $MONITOR_MS --ipmi-pass-file $IPMI_PASS --ipmi-ip-addr $IPMI_ADDR &

monitor_pid=$!
sleep 5s

outlog="$sub_dir/bash-times.log"
echo "action", "CPUs", "datetime" > $outlog

# https://stackoverflow.com/questions/6481005/how-to-obtain-the-number-of-cpus-cores-in-linux-from-the-command-line
# PHYSICAL_CORES=$(lscpu -p | egrep -v '^#' | sort -u -t, -k 2,4 | wc -l)
CORES=$(nproc --all)
for CPUS in $(seq 1 $CORES); do
  echo "start", "$CPUS", `TZ=UTC date` >> $outlog
  ./lookbusy-1.4/lookbusy --cpu-mode=fixed --cpu-util=90 --ncpus=$CPUS --mem-util=1GB > /dev/null &
  lookbusy_pid=$!
  sleep 20s
  echo "stop", "$CPUS", `TZ=UTC date` >> $outlog
  kill $lookbusy_pid

  sleep 5s
done

kill $monitor_pid
python3 combine-logs.py --logs-folder $sub_dir --energy-freq-ms $MONITOR_MS
python3 plot-association.py --logs-folder $sub_dir

##################################################################################################################################
# Memory
sub_dir="$OUT_DIR/mem"
mkdir -p $sub_dir
# waiting for IPMI to return to normal
sleep 5s

../../../Ilúvatar/target/release/iluvatar_energy_mon --enable-rapl --enable-ipmi --enable-perf --log-folder $sub_dir \
--log-freq-ms $MONITOR_MS --perf-stat-duration-ms $MONITOR_MS --ipmi-pass-file $IPMI_PASS --ipmi-ip-addr $IPMI_ADDR &

monitor_pid=$!
sleep 5s

outlog="$sub_dir/bash-times.log"
echo "action", "MEM", "datetime" > $outlog

mem_mb=$(free -m | grep "Mem" | awk '{ print $2 }')
step=$(($mem_mb / 10))
stop=$(($mem_mb - $step))
for MEM in $(seq $step $step $stop); do
  echo "start", "$MEM", `TZ=UTC date` >> $outlog
  ./lookbusy-1.4/lookbusy --cpu-mode=fixed --cpu-util=10 --ncpus=1 --mem-util="$MEM"MB > /dev/null &
  lookbusy_pid=$!
  sleep 20s
  echo "stop", "$MEM", `TZ=UTC date` >> $outlog
  kill $lookbusy_pid

  sleep 5s
done

kill $monitor_pid
python3 combine-logs.py --logs-folder $sub_dir --energy-freq-ms $MONITOR_MS
python3 plot-association.py --logs-folder $sub_dir
