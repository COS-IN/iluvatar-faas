#!/bin/bash

sudo mv ./libnvshare.so /usr/local/lib/ 
sudo ldconfig /usr/local/lib

if false; then 
  sudo bash -c 'echo -ne "\n/usr/local/lib/libnvshare.so" >> /etc/ld.so.preload'
fi
