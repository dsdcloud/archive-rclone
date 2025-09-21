import subprocess
import os
import logging

logger = logging.getLogger(__name__)

def rclone_list_remotes(rclone_conf_path: str) -> list:
    if not os.path.exists(rclone_conf_path):
        return []
    remotes = []
    with open(rclone_conf_path, 'r', encoding='utf-8') as fh:
        for line in fh:
            line = line.strip()
            if line.startswith('[') and line.endswith(']'):
                remotes.append(line[1:-1])
    return remotes

def rclone_copy(local_path: str, remote_and_path: str, rclone_conf_path: str, extra_args=None):
    if extra_args is None:
        extra_args = []
    cmd = ['rclone', 'copy', '--progress', local_path, remote_and_path, '--config', rclone_conf_path]
    cmd += ['--transfers', '4', '--checkers', '8', '--drive-chunk-size', '32M']
    cmd += extra_args
    logger.info("Running rclone: %s", ' '.join(cmd))
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        logger.error("rclone error: %s", p.stderr)
        raise RuntimeError(f"rclone failed: {p.stderr or p.stdout}")
    return p.stdout
