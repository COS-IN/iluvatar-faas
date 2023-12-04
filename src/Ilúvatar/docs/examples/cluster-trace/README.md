# Trace

In this example we bring up the entire cluster: controller, worker, and time series database.
Notice how the `ansible-playbook` command in [run.sh](./run.sh) is running `iluvatar.yml`, whereas [examples with just the worker](../benchmark/run.sh) run `worker.yml`.
Each is hosted on your local machine, but Ansible connects to it via ssh.
When deploying to multiple nodes, ssh keys must be set up on each remote node to allow Ansible to connect and run on them.
If the key needs a password, as ours does, using `sshpass` to provide the password or [configure it via Ansible](https://stackoverflow.com/a/44734246) is required.

**NOTE:** this example requires `sshpass` to be installed on your machine.
Run `sudo apt install sshpass -y` to set up the dependency.

Make sure you've updated the [host_addresses file](../../../ansible/group_vars/host_addresses.yml) with your machine's networking interface.
Simply execute `./run.sh` to run the example.
