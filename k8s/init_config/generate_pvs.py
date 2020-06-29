import sys, os

def generate_k8s_pvs(count):
    for i in range(count):
        item = f'0{i}' if i < 10 else str(i)
        pvConfig = f"""
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pyql-pv-volume-{item}
  labels:
    type: local
spec:
  storageClassName: pyql-pv-manual-sc
  capacity:
    storage: 20Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: /mnt/pyql-pv-volume-{item}
EOF
"""
        # apply configuraiton
        os.system(f"cat <<EOF | kubectl apply -f - {pvConfig}")

if __name__ == '__main__':
    assert len(sys.argv) < 3, "missing argument for <pv count>"
    generate_k8s_pvs(int(sys.argv[1]))
