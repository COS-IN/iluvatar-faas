import os
import subprocess
import json
import traceback
from copy import deepcopy
import shutil
from enum import Enum
from typing import Dict

class RunTarget(Enum):
    WORKER = "worker"
    CONTROLLER = "controller"

    def __str__(self) -> str:
        return self.value

    def yml(self) -> str:
        if self == self.WORKER:
            return "worker.yml"
        elif self == self.CONTROLLER:
            return "iluvatar.yml"


def _run_ansible_clean(log_file, kwargs):
    run_args = [
        "ansible-playbook",
        "-i",
        kwargs["ansible_host_file"],
        os.path.join(kwargs["ilu_home"], "ansible", kwargs["target"].yml()),
    ]
    env_args = [kwargs["ansible_hosts_addrs"], "mode=clean", f"target={kwargs['build_level'].path_name()}"]

    worker_env = kwargs.to_env_var_dict("worker")
    worker_env = json.dumps({"worker_environment": worker_env})
    env_args.append(worker_env)
    if kwargs["target"] == RunTarget.CONTROLLER:
        controller_env = kwargs.to_env_var_dict("controller")
        controller_env = json.dumps({"controller_environment": controller_env})
        env_args.append(controller_env)

    for env_arg in env_args:
        run_args.append("-e")
        run_args.append(env_arg)

    if kwargs["private_ssh_key"] is not None:
        run_args.append(f"--private-key={kwargs['private_ssh_key']}")
    _run_cmd(run_args, log_file)


def _copy_logs(log_file, results_dir, kwargs):
    if kwargs["host"] == "localhost" or kwargs["host"] == "127.0.0.1":
        if os.path.isdir(kwargs["worker_log_dir"]):
            for subdir in os.listdir(kwargs["worker_log_dir"]):
                src = os.path.join(kwargs["worker_log_dir"], subdir)
                dest = os.path.join(results_dir, subdir)
                if src != dest:
                    shutil.move(src, dest)
        if os.path.isdir(kwargs["controller_log_dir"]):
            for subdir in os.listdir(kwargs["controller_log_dir"]):
                src = os.path.join(kwargs["controller_log_dir"], subdir)
                dest = os.path.join(results_dir, subdir)
                if src != dest:
                    shutil.move(src, dest)
    else:
        from paramiko import SSHClient
        import paramiko
        from scp import SCPClient

        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.load_system_host_keys()
            ssh.load_host_keys(os.path.expanduser("~/.ssh/known_hosts"))
            ssh.connect(kwargs["host"])
            with SCPClient(ssh.get_transport(), sanitize=lambda x: x) as scp:
                scp.get(f"{kwargs['worker_log_dir']}/*", results_dir, recursive=True)


def _pre_run_cleanup(
    log_file,
    results_dir,
    kwargs,
):
    _run_ansible_clean(log_file, kwargs)
    clean_dir = os.path.join(results_dir, "precleanup")
    os.makedirs(clean_dir, exist_ok=True)
    _copy_logs(log_file, clean_dir, kwargs)


def _remote_cleanup(
    log_file,
    results_dir,
    kwargs,
):
    print("Cleanup:", results_dir)
    _copy_logs(log_file, results_dir, kwargs)
    _run_ansible_clean(log_file, kwargs)

    if kwargs["host"] == "localhost" or kwargs["host"] == "127.0.0.1":
        if results_dir != kwargs["worker_log_dir"]:
            _run_cmd(["rm", "-rf", kwargs["worker_log_dir"]], log_file, shell=False)
    else:
        from paramiko import SSHClient
        import paramiko
        from scp import SCPClient

        with SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.load_system_host_keys()
            ssh.load_host_keys(os.path.expanduser("~/.ssh/known_hosts"))
            ssh.connect(kwargs["host"])
            ssh.exec_command(f"sudo rm -rf {kwargs['worker_log_dir']}")


def _run_cmd(cmd_args, log_file, shell: bool = False, env: Dict = None):
    opened_log = False
    if type(log_file) is str:
        log_file = open(log_file, "a")
        opened_log = True
    try:
        formatted_args = []
        for x in cmd_args:
            string = str(x)
            if string.startswith("-e") and string != "-e":
                raise Exception(f"Bad ansible argument: {string}")

            formatted_args.append(string)
        sys_env = deepcopy(os.environ)
        sys_env["RUST_BACTRACE"] = "1"
        if env is not None:
            sys_env = {**sys_env, **env}
        completed = subprocess.run(
            args=formatted_args,
            stdout=log_file,
            stderr=log_file,
            text=True,
            shell=shell,
            env=sys_env,
        )
        completed.check_returncode()
        return completed.stdout
    except Exception as e:
        if log_file is not None:
            log_file.write("Exception encountered:\n")
            log_file.write(str(e))
            log_file.write("\n")
            log_file.write(traceback.format_exc())
            log_file.write("\n")
            for arg in cmd_args:
                log_file.write(f"{arg} ")
                log_file.write("\n")
            if env is not None:
                log_file.write(json.dumps(env))
                log_file.write("\n")

        raise e
    finally:
        if opened_log:
            log_file.close()


def _run_ansible(
    log_file,
    kwargs,
):
    env_args = [
        kwargs["ansible_hosts_addrs"],
        "mode=deploy",
        f"target={kwargs['build_level'].path_name()}",
    ]
    worker_env = kwargs.to_env_var_dict("worker")
    worker_env = json.dumps({"worker_environment": worker_env})
    env_args.append(worker_env)
    if kwargs["target"] == RunTarget.CONTROLLER:
        controller_env = kwargs.to_env_var_dict("controller")
        controller_env = json.dumps({"controller_environment": controller_env})
        env_args.append(controller_env)
    if kwargs["target"] == RunTarget.CONTROLLER:
        env_args.append("cluster=true")

    run_args = [
        "ansible-playbook",
        "-i",
        kwargs["ansible_host_file"],
        os.path.join(kwargs["ilu_home"], "ansible", kwargs["target"].yml()),
    ]
    if kwargs["private_ssh_key"] is not None:
        run_args.append(f"--private-key={kwargs['private_ssh_key']}")

    for env_arg in env_args:
        run_args.append("-e")
        run_args.append(env_arg)

    if type(kwargs["ansible_args"]) == str and len(kwargs["ansible_args"]) > 0:
        ansible_args = kwargs["ansible_args"].split(" ")
        run_args.extend(ansible_args)
    elif type(kwargs["ansible_args"]) == list and len(kwargs["ansible_args"]) > 0:
        run_args.extend(kwargs["ansible_args"])
    _run_cmd(run_args, log_file)
