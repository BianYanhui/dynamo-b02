#!/bin/bash
# Dynamo cluster UP: frontend (kv routing) + 4 vLLM workers, one per T4.
# Usage: bash /home/byh/Dynamo/cluster_up.sh
#   KV_EVENTS=1  -> workers publish KV block events on ZMQ ports 20081-20084
#   GPU_MEM_UTIL=0.60 -> adjust per-worker VRAM fraction
#   DYN_B02_PREPUBLISH=1 -> filter KV events before the Worker ZMQ PUB
set -u
cd /home/byh/Dynamo
source .venv-dynamo/bin/activate
export DYN_DISCOVERY_BACKEND=file
export HF_HUB_OFFLINE=1
NV13=/home/byh/Dynamo/.venv-dynamo/lib/python3.12/site-packages/nvidia/cu13/lib
export LD_LIBRARY_PATH=/home/byh/cuda13-compat:$NV13${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

MODEL=Qwen/Qwen2.5-1.5B-Instruct
GPUMEM=${GPU_MEM_UTIL:-0.90}
KV_EVENTS=${KV_EVENTS:-0}
DYN_B02_PREPUBLISH=${DYN_B02_PREPUBLISH:-0}
WORKER_ENV=()
if [ "$DYN_B02_PREPUBLISH" = "1" ]; then
  WORKER_ENV=(DYN_B02_WORKER=1 PYTHONPATH="/home/byh/Dynamo/dynamo/b02${PYTHONPATH:+:$PYTHONPATH}")
fi

# frontend
setsid nohup python -m dynamo.frontend --http-port 8000 --router-mode kv \
    --no-router-kv-events > /tmp/dyn_frontend.log 2>&1 < /dev/null &

# workers (GPU 0..3)
for G in 0 1 2 3; do
  EXTRA=""
  if [ "$KV_EVENTS" = "1" ]; then
    PORT=$((20081 + G))
    EXTRA="--kv-events-config {\"publisher\":\"zmq\",\"topic\":\"kv-events\",\"endpoint\":\"tcp://*:$PORT\",\"enable_kv_cache_events\":true}"
  fi
  env "${WORKER_ENV[@]}" CUDA_VISIBLE_DEVICES=$G setsid nohup python -m dynamo.vllm \
      --model $MODEL --dtype half --gpu-memory-utilization $GPUMEM \
      $EXTRA \
      > /tmp/dyn_worker_$G.log 2>&1 < /dev/null &
done

# wait until 4 generate instances are registered
for i in $(seq 1 120); do
  n=$(curl -s --max-time 3 http://localhost:8000/health 2>/dev/null | python3 -c "
import json,sys
try: print(sum(1 for i in json.load(sys.stdin)['instances'] if i['endpoint']=='generate'))
except Exception: print(0)" 2>/dev/null)
  [ "${n:-0}" -ge 4 ] && { echo "cluster UP: frontend + $n workers (kv_events=$KV_EVENTS)"; exit 0; }
  sleep 3
done
echo "TIMEOUT waiting for workers; check /tmp/dyn_worker_*.log"
exit 1
