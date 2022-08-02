# Ansible

## Environment Variables

`ILU_HOME` - can be set to specify the folder where the ansible structure is.
This must be specified, or the commands must be executed in that directory.

## Setup

Deploy all services on the local machine. 
Currently need to adjust paths to the executables.

```
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=clean
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=deploy
```

**MUST** run clean first if run previously on a machine.
The dangling worker/Graphite service exchange causes issues and the load stats stops being registered.

## Tear down

Clean up services on the local machine. 
```
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=clean
```
