#!/bin/bash
# Event-volume A/B: same agentic shape under three routing modes with KV
# events ENABLED on workers. Measures the offered load of the ingestion
# funnel (issue #11899) per routing mode.
#   cell rr  : frontend round-robin, client -> workers directly
#   cell kv  : frontend kv-router (events consumed natively)
#   cell b02 : frontend -> B02 sketch router (affinity pin)
set -u
export DYN_DISCOVERY_BACKEND=file
export HF_HUB_OFFLINE=1
NV13=/home/byh/Dynamo/.venv-dynamo/lib/python3.12/site-packages/nvidia/cu13/lib
export LD_LIBRARY_PATH=/home/byh/cuda13-compat:$NV13${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
REPO=/home/byh/Dynamo/dynamo
B02=$REPO/b02
SNAPSHOT=/home/byh/.cache/huggingface/hub/models--Qwen--Qwen2.5-1.5B-Instruct/snapshots/989aa7980e4cf806f80c7fef2b1adb7bc71aa306
STATE_DIR=/tmp/b02_events_ab
MODEL=Qwen/Qwen2.5-1.5B-Instruct
mkdir -p $STATE_DIR

wait_frontend() {
  for i in $(seq 1 90); do
    n=$(curl -s --max-time 3 http://localhost:8000/health 2>/dev/null | python3 -c "
import json,sys
try: print(sum(1 for i in json.load(sys.stdin)['instances'] if i['endpoint']=='generate'))
except Exception: print(0)" 2>/dev/null)
    [ "${n:-0}" -ge 4 ] && return 0
    sleep 2
  done
  echo "frontend not ready"; return 1
}

start_frontend() { # $1 mode $2 extra flags
  pkill -f 'dynamo[.]frontend' 2>/dev/null; sleep 2
  cd /home/byh/Dynamo && source .venv-dynamo/bin/activate
  setsid nohup python -m dynamo.frontend --http-port 8000 --router-mode $1 \
      $2 > /tmp/dyn_frontend.log 2>&1 < /dev/null &
  wait_frontend || exit 1
  sleep 1
}

stop_router() { pkill -f 'b02_sketch[_]router' 2>/dev/null; sleep 2; }

start_router() {
  cd $REPO && source /home/byh/Dynamo/.venv-dynamo/bin/activate
  setsid nohup env PYTHONPATH=$B02 python -m b02_sketch_router \
      --endpoint dynamo.backend.generate --model-name $MODEL \
      --model-path $SNAPSHOT --served-model-name qwen-b02 \
      --state-log-dir /tmp/b02_state_logs \
      > /tmp/b02_router.log 2>&1 < /dev/null &
  for i in $(seq 1 30); do
    MODELS=$(curl -s --max-time 3 http://localhost:8000/v1/models 2>/dev/null | python3 -c "
import json,sys
try: print(','.join(m.get('id','') for m in json.load(sys.stdin).get('data',[])))
except Exception: print('')" 2>/dev/null)
    case "$MODELS" in *qwen-b02*) echo "  router serving qwen-b02"; return 0;; esac
    sleep 2
  done
  echo "FAIL: qwen-b02 not served"; return 1
}

probe() { # wait out stale discovery pool entries
  for i in 1 2 3 4 5 6; do
    CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 \
      http://localhost:8000/v1/chat/completions -H 'Content-Type: application/json' \
      -H "x-dynamo-session-id: probe_$1_$i" \
      -d "{\"model\":\"$2\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":4}")
    [ "$CODE" = "200" ] && { echo "  probe: 200"; return 0; }
    echo "  probe $i: $CODE"; sleep 4
  done
  return 1
}

start_tap() { # $1 tag
  setsid nohup python $B02/smoke/event_tap.py \
      --ports 20081,20082,20083,20084 --duration 999999 \
      --out $STATE_DIR/tap_$1.jsonl > /tmp/tap_$1.log 2>&1 < /dev/null &
  echo $! > /tmp/tap_$1.pid
  sleep 1
}
stop_tap() { kill $(cat /tmp/tap_$1.pid) 2>/dev/null; sleep 1; }

run_load() { # $1 tag $2 model $3 seed
  python $B02/smoke/smoke_client.py --model $2 --n-workflows 8 --steps 8 \
      --seed $3 --url http://localhost:8000/v1/chat/completions \
      --out $STATE_DIR/results_events_$1.jsonl
}

echo "=== [0] cluster up with KV events enabled ==="
export KV_EVENTS=1
if pgrep -f 'dynamo[.]frontend' >/dev/null && [ "$(pgrep -cf 'dynamo[.]vllm' 2>/dev/null || echo 0)" -ge 4 ]; then
  echo "  cluster already up — but events flag may differ; restarting anyway"
  bash /home/byh/Dynamo/cluster_down.sh
fi
bash /home/byh/Dynamo/cluster_up.sh || exit 1
grep -h 'use_kv_events' /tmp/dyn_worker_0.log | head -1

echo "=== [cell 1/3] round-robin (scatter baseline) ==="
stop_router
start_frontend round-robin ""
start_tap rr
run_load rr $MODEL 11
stop_tap rr

echo "=== [cell 2/3] native kv-router (events consumed natively) ==="
start_frontend kv ""
start_tap kv
run_load kv $MODEL 12
stop_tap kv

echo "=== [cell 3/3] B02 sketch router (affinity pin) ==="
stop_router
start_frontend round-robin ""
start_router || exit 1
probe b02 qwen-b02 || exit 1
start_tap b02
run_load b02 qwen-b02 13
stop_tap b02

echo "=== analysis ==="
python $B02/smoke/analyze_events.py
echo "EVENT AB DONE"
