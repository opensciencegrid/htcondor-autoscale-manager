
import json
import subprocess

def patch_annotation(pod, value):
    patch = json.dumps({"metadata": {"annotations": {"controller.kubernetes.io/pod-deletion-cost": value}}})
    result = subprocess.run(["kubectl", "patch", "pod", "-p", "patch"], capture_output=True)
    result.check_returncode()
