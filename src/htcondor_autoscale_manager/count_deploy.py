
import subprocess

def count_deploy(query, resource, pool=None):

    count = subprocess.run(["/app/kubectl", "-o", "json", "-l", query], capture_output=True)

    count.check_returncode()

    pods = set()
    for pod in count['items']:
        pods.add(pod['metadata']['name'])

    pslots = coll.query(htcondor.AdTypes.Startd,
        constraint="GLIDEIN_ResourceName=?=%s && PartitionableSlot && Offline =!= true" % \
                   classad.quote(resource),
        projection=["TotalCPUs", "CPUs", "Name", "UtsnameNodename"])

    online_pods = set()
    idle_pods = set()
    for slot in pslots:
        name = pslots.get("UtsnameNodename")
        if not name:
            continue
        online_pods.add(name)
        if slot.get("CPUs") == slot.get("TotalCpus"):
            idle_slots.add(name)

    return {"pods": pods,
            "idle_pods": idle_pods,
            "online_pods": online_pods,
            "total": len(pods.union(online_pods)),
            "idle": len(idle_slots),
            "offline": pods.difference(online_pods),
           }
