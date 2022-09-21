# Frequent Errors & Fixes

1. `missing parent` when pulling ctr image
  * delete all the images present in `ctr`,via `sudo ctr images rm $(sudo ctr images ls -q)`
  * and possibly re-make zfs pool
2. `veth0` exists, or networking bridge error
  * run `clean` option of ilúvatar_worker