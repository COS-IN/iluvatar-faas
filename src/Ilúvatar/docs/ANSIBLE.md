# Ansible

Ilúvatar uses [Ansible](https://docs.ansible.com/ansible/latest/index.html) for easy deployment of all the pieces that make up the control plane.
Ansible works by running a series of [playbooks](https://docs.ansible.com/ansible/latest/user_guide/playbooks_intro.html) on different host which do the work of setting up services.
Which playbook can be specified in the Ansible command, i.e.`ansible-playbook controller.yml` only runs the controller playbook.

The playbooks also implicitly list out the startup ordering for all the services, as incorrect ordering can result in errors or total failures of parts of the system.

It is generally easiest to run commands from inside the [Ansible](../ansible/) directory, but that is not necessary.
You can set the `ILU_HOME` environment variable to specify the folder where the Ansible structure is.
One of these must be fulfilled or the commands will fail.
Make sure to install the correct version of Ansible, in the Ansible directory's [reqs.txt](../ansible/reqs.txt) file.

## Placing services on hosts

Where each playbook is run on is specified by an environments file, some examples of which are all located [here](../ansible/environments/).
The address of a host is specified with `ansible_host`, and the type of connection (`local` or `ssh`) is done via `ansible_connection`.
If using ssh, Ansible will expect an ssh key to be set up that allow connection to all the hosts listed.

## Configuration via Ansible

Most of the configuration uses the default json files for both [controller](../iluvatar_controller/src/controller.json) and [worker](../iluvatar_worker/src/worker.json).
Configuration of both can be overloaded at _runtime_ using environment variables as described in both [worker](./WORKER.md) and [controller](./CONTROLLER.md) sections, and Ansible can take advantage of that.
The tasks that start the executables and set their environment, allowing injection through Ansible.
The startup task (`run worker executable`) for the [worker](../ansible/worker.yml) has a number of examples of this.

Variables can be passed to `ansible-playbook` using the `-e` flag which are then populated in the various playbooks.
For example, adding a setting to configure enable RAPL energy readings could be done by passing `-e worker_enable_rapl=true` to `ansible-playbook`.
Inside the worker playbook, a matching line would have to be added to read this variable and pass it into the worker: `"ILUVATAR_WORKER__energy__rapl_freq_ms" : "{{ worker_rapl_log_freq_ms | default(1000) }}"`.

## Multiple remote Worker/host configuration

Each worker requires unique host-specific information that cannot be supplied by a simple variable passed to ansible via `-e`.
They require the IP address on the machine to attach to for receiving connections, and optionally the address where IPMI can be reached.
This must be done from a more complex YAML file that Ansible can import at runtime.

The data must be in the following format. With an example being located [here](../ansible/group_vars/host_addresses.yml)

```YAML
servers:
  <HOSTNAME>:
    internal_ip: <IP_ADDR>
    ipmi_ip: <IP_ADDR>
```

The `hostname` **must** match the `ansible_host` values in your matching environment file.
The example file is loaded by default, and provides a match for `127.0.0.1`, i.e. the localhost `ansible_host` hostname.

To include these variables, they can be passed to the `ansible-playbook` command like this:
`-e @../path/to/custom/host_addresses.yml`.
The `@` is important, specifying that the file should be extracted into multiple variables.
Each host in your environment is then supplied with the values specified auto-magically.

For writing new playbooks, such variables can simply be extraced like this: `servers[ansible_host].internal_ip`.

## Running individual commands

You **MUST** run the clean command first if the services have been set up before on the target machine(s).

The hosts you want to run services on must be in an environments file, and given to Ansible via `-i`.

A simple command to run Ilúvatar on the local machine.

```sh
# clean the environment to remove any possible leftover parts 
ansible-playbook -i environments/local/hosts.ini iluvatar.yml -e mode=clean -e "@./group_vars/host_addresses.yml"
# Deploy the system
ansible-playbook -i environments/local/hosts.ini iluvatar.yml -e mode=deploy -e "@./group_vars/host_addresses.yml"
```

### Tear down

To clean up services without starting them again, just pass `mode=clean`.

```sh
ansible-playbook -i environments/local/hosts.ini iluvatar.yml -e mode=clean -e "@./group_vars/host_addresses.yml"
```
