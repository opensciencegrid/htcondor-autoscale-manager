
import json
import subprocess

def patch_annotation(pod, value):
    patch = json.dumps({"metadata": {"annotations": {"controller.kubernetes.io/pod-deletion-cost": str(value)}}})
    result = subprocess.run(["/app/kubectl", "patch", "pod", pod, "-p", patch], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    result.check_returncode()
