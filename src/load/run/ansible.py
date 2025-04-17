import os
import subprocess
import json
import traceback
from copy import deepcopy
import shutil
from enum import Enum
from typing import Dict
from logging import Logger

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


def _run_ansible_clean(logger: Logger, kwargs):
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
    _run_cmd(run_args, logger)


def _copy_logs(logger: Logger, results_dir, kwargs):
    logger.info(f"Copying remote logs to {results_dir}")
    run_args = [
        "ansible-playbook",
        "-i",
        kwargs["ansible_host_file"],
        os.path.join(kwargs["ilu_home"], "ansible", kwargs["target"].yml()),
    ]
    env_args = [kwargs["ansible_hosts_addrs"], "mode=logs", f"log_copy_dir={results_dir}", f"worker_log_dir={kwargs['worker_log_dir']}/"]
    if kwargs["target"] == RunTarget.CONTROLLER:
        env_args.append(f"controller_log_dir={kwargs['controller_log_dir']}/")
    for env_arg in env_args:
        run_args.append("-e")
        run_args.append(env_arg)
    if kwargs["private_ssh_key"] is not None:
        run_args.append(f"--private-key={kwargs['private_ssh_key']}")
    _run_cmd(run_args, logger)

def _pre_run_cleanup(
    logger: Logger,
    results_dir,
    kwargs,
):
    _run_ansible_clean(logger, kwargs)
    clean_dir = os.path.join(results_dir, "precleanup")
    os.makedirs(clean_dir, exist_ok=True)
    _copy_logs(logger, clean_dir, kwargs)


def _remote_cleanup(
    logger: Logger,
    results_dir,
    kwargs,
):
    print("Cleanup:", results_dir)
    _copy_logs(logger, results_dir, kwargs)
    _run_ansible_clean(logger, kwargs)

    if kwargs["host"] == "localhost" or kwargs["host"] == "127.0.0.1":
        if results_dir != kwargs["worker_log_dir"]:
            _run_cmd(["rm", "-rf", kwargs["worker_log_dir"]], logger, shell=False)
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


def _run_cmd(cmd_args, logger: Logger, shell: bool = False, env: Dict = None):
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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=shell,
            env=sys_env,
        )
        logger.info(completed.stdout)
        logger.info(completed.stderr)
        completed.check_returncode()
        return completed.stdout
    except Exception as e:
        args = "Args: "
        for arg in cmd_args:
            args += f"{arg} \n"
        env = f"Cust env: {json.dumps(env)}"
        msg = "\n".join([
            "Exception encountered:",
            str(e),
            traceback.format_exc(),
            args,
            env,
        ])
        logger.error(msg)
        raise e


def _run_ansible(
    logger: Logger,
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
    _run_cmd(run_args, logger)
