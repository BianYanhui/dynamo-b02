#!/bin/bash
# B02 sketch-router smoke test:
#   cluster (4 workers + frontend) -> b02 sketch router -> agentic load -> analysis
set -u
export DYN_DISCOVERY_BACKEND=file
export HF_HUB_OFFLINE=1
NV13=/home/byh/Dynamo/.venv-dynamo/lib/python3.12/site-packages/nvidia/cu13/lib
export LD_LIBRARY_PATH=/home/byh/cuda13-compat:$NV13${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
REPO=/home/byh/Dynamo/dynamo
B02=$REPO/b02
MODEL_SNAPSHOT=/home/byh/.cache/huggingface/hub/models--Qwen--Qwen2.5-1.5B-Instruct/snapshots/989aa7980e4cf806f80c7fef2b1adb7bc71aa306
STATE_DIR=/tmp/b02_state_logs

echo "[1/5] cluster up (workers + frontend)"
if pgrep -f 'dynamo[.]frontend' >/dev/null && [ "
$(pgrep -cf 'dynamo[.]vllm' 2>/dev/null || echo 0)" -ge 4 ]; then
  echo "  cluster already up, skipping"
else
  bash /home/byh/Dynamo/cluster_up.sh || exit 1
fi

echo "[2/5] start b02 sketch router"
mkdir -p $STATE_DIR
pkill -f 'b02_sketch[_]router' 2>/dev/null; sleep 1
# Wait out file-discovery removal propagation: the frontend's worker pool
# keeps a dead handler for ~8s; requests routed there fail with 500.
echo "  waiting for discovery to forget any dead router instance..."
sleep 12
cd $REPO
source /home/byh/Dynamo/.venv-dynamo/bin/activate
setsid nohup env PYTHONPATH=$B02 python -m b02_sketch_router \
    --endpoint dynamo.backend.generate \
    --model-name Qwen/Qwen2.5-1.5B-Instruct \
    --model-path $MODEL_SNAPSHOT \
    --served-model-name qwen-b02 \
    --state-log-dir $STATE_DIR \
    --tick-seconds 2 \
    > /tmp/b02_router.log 2>&1 < /dev/null &

echo "[3/5] wait for served model"
for i in $(seq 1 30); do
  MODELS=$(curl -s --max-time 3 http://localhost:8000/v1/models 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    print(','.join(m.get('id','') for m in d.get('data',[])))
except Exception: print('')" 2>/dev/null)
  case "$MODELS" in *qwen-b02*) echo "  router serving: $MODELS"; break;; esac
  sleep 2
done
case "${MODELS:-}" in *qwen-b02*) ;; *) echo "FAIL: qwen-b02 not served; router log:"; tail -20 /tmp/b02_router.log; exit 1;; esac

# probe: one tiny request must return 200 before the real load
for i in 1 2 3 4 5; do
  CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 \
    http://localhost:8000/v1/chat/completions -H 'Content-Type: application/json' \
    -H "x-dynamo-session-id: probe_$i" \
    -d '{"model":"qwen-b02","messages":[{"role":"user","content":"hi"}],"max_tokens":4}')
  [ "$CODE" = "200" ] && { echo "  probe $i: 200 OK"; break; }
  echo "  probe $i: $CODE (waiting out stale pool entry)"; sleep 4
done
[ "$CODE" = "200" ] || { echo "FAIL: probe never succeeded"; exit 1; }

echo "[4/5] agentic smoke load (8 workflows x 8 steps, session headers)"
python $B02/smoke/smoke_client.py \
    --model qwen-b02 --n-workflows 8 --steps 8 \
    --out $STATE_DIR/smoke_results.jsonl

echo "[5/5] analysis"
python $B02/smoke/analyze_smoke.py
RC=$?
echo "router decisions log: $STATE_DIR/decisions.jsonl"
echo "router log: /tmp/b02_router.log"
exit $RC
