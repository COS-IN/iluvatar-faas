# Ansible

Ilúvatar uses [Ansible](https://docs.ansible.com/ansible/latest/index.html) for easy deployment of all the pieces that make up the control plane.
Ansible works by running a series of [playbooks](https://docs.ansible.com/ansible/latest/user_guide/playbooks_intro.html) on different host which do the work of setting up services.
Which playbook can be specified in the Ansible command, i.e.`ansible-playbook controller.yml` only runs the controller playbook.

The playbooks also implicitly list out the startup ordering for all the services, as incorrect ordering can result in errors or total failures of parts of the system.

It is generally easiest to run commands from inside the [Ansible](../ansible/) directory, but that is not necessary.
You can set the `ILU_HOME` variable to specify the folder where the Ansible structure is.
One of these must be fulfilled or the commands will fail.

## Placing services on hosts

Where each palybook is run on is specified by an environments file, which are all located [here](../ansible/environments/).
The address of a host is specified with `ansible_host`, and the type of connection (`local` or `ssh`) is done via `ansible_connection`.
If using ssh, Ansible will expect an ssh key to be set up that allow connection to all the hosts listed.

## Configuration via Ansible

Most of the configuration uses the defaul json files for both [controller](../ilúvatar_controller/src/controller.json) and [worker](../ilúvatar_worker/src/worker.json).
Configuration of both can be overloaded at _runtime_ using environment variables as described in the [config](./CONFIG.md) section, and Ansible can take advantage of that.
The tasks that start the executables and set their environment, allowing injection through Ansible.
The startup task (`run worker executable`) for the [worker](../ansible/worker.yml) has a number of examples of this.

Variables can be passed to ansible-playbook using the `-e` flag which are then populated in the various playbooks.
For example, adding a setting to configure enable rapl energy readings could be done by passing `-e worker_enable_rapl=true` to ansible-playbook.
Inside the worker playbook, a matching line would have to be added to read this variable and pass it into the worker: `"ILUVATAR_WORKER__energy__enable_rapl" : "{{ worker_enable_rapl | default(false) }}"`.

## Running commands

You **MUST** run the clean command first if the services have been set up before on the target machine(s).
Leftover resources and the dangling worker/Graphite service exchange causes issues resource exhaustion and load stats reporting stops working.

The hosts you want to run services on must be in an environments file, and given to Ansible via `-i`.

A simple command to run Ilúvatar on the local machine.
```sh
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=clean
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=deploy
```

### Tear down

To clean up services without starting them again, just pass `mode=clean`. 
```sh
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=clean
```
