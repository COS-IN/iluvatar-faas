# Ansible


## Setup

Deploy all services on the local machine. 
Currently need to adjust paths to the executables.

```
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=deploy
```

## Tear down

Clean up services on the local machine. 
```
ansible-playbook -i environments/local/hosts.ini ilúvatar.yml -e mode=clean
```
