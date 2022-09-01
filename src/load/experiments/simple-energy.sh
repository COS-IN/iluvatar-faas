#!/bin/bash

# combines the energy monitor with lookbusy to get a siple test of usage and a baseline value
# to run:
# ./simple-energy.sh -i=127.0.0.200 --ipmi-pass-f=~/ipmipass -c=20

CPUS=1
for i in "$@"
do
case $i in
    -i=*|--ipadddr=*)
    IPMI_ADDR="${i#*=}"
    ;;
    --ipmi-pass-f=*)
    IPMI_PASS="${i#*=}"
    ;;
    -c=*|--cpus=*)
    CPUS="${i#*=}"
    ;;
    *)
      # unknown option
    ;;
esac
done

if [ ! -f ./lookbusy-1.4/lookbusy ]
then
# set up lookbusy if missing
wget http://www.devin.com/lookbusy/download/lookbusy-1.4.tar.gz && tar -xvf lookbusy-1.4.tar.gz \
  && rm lookbusy-1.4.tar.gz && cd ./lookbusy-1.4 && ./configure && make && cd ..
fi

# waiting for IPMI to return to normal
sleep 5s

../../Ilúvatar/target/release/ilúvatar_energy_mon --enable-rapl --enable-ipmi --enable-perf --log-folder . \
 --log-freq-ms 1000 --perf-stat-duration-ms 1000 --ipmi-pass-file $IPMI_PASS --ipmi-ip-addr $IPMI_ADDR &

monitor_pid=$!
sleep 5s

echo "start", `date` >> bash-times.log
./lookbusy-1.4/lookbusy --cpu-mode=fixed --cpu-util=90 --ncpus=$CPUS --mem-util=10GB > /dev/null &
lookbusy_pid=$!
sleep 20s
echo "stop", `date` >> bash-times.log
kill $lookbusy_pid

sleep 5s

kill $monitor_pid
