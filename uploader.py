import subprocess
import os
import logging

logger = logging.getLogger(__name__)

# Rclone Auth Error အတွက် Custom Exception
class RcloneAuthError(RuntimeError):
    pass

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
    
    # rclone command ကိုတည်ဆောက်ခြင်း
    cmd = ['rclone', 'copy', '--progress', local_path, remote_and_path, '--config', rclone_conf_path]
    cmd += ['--transfers', '4', '--checkers', '8', '--drive-chunk-size', '32M']
    cmd += extra_args
    
    logger.info("Running rclone: %s", ' '.join(cmd))
    
    # rclone ကို run ပြီး output များကို ဖမ်းယူခြင်း
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    if p.returncode != 0:
        logger.error("rclone error: %s", p.stderr)
        error_output = p.stderr or p.stdout
        
        # Token သက်တမ်းကုန် error ကို စစ်ဆေးခြင်း
        if "invalid_grant" in error_output or "maybe token expired" in error_output:
            raise RcloneAuthError(f"Rclone authentication failed. Token expired for remote: {remote_and_path.split(':')[0]}")

        # အခြား rclone error များ
        raise RuntimeError(f"rclone failed: {error_output}")
        
    return p.stdout
