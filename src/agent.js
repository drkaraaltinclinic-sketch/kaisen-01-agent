/**
 * KAIZEN-01 — 改善 The Continuous Improvement Engine
 * Listens to everything, remembers across days, and generates a prioritized
 * improvement backlog for the whole network:
 *   observe (bus telemetry + trend memory)
 *   → propose (Claude generates tasks: target, remedy, rationale, impact)
 *   → deliver (tasks broadcast on the bus + tracked on the board)
 *   → measure (after a task is deployed, compare before/after metrics)
 * CONSTITUTION: Kaizen PROPOSES, humans DISPOSE — it never deploys code itself.
 */

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const fetch = require('node-fetch');
const http = require('http');
const cors = require('cors');
const path = require('path');

const PORT = process.env.PORT || 3021;
const GECKO_URL = process.env.GECKO_URL || 'wss://gecko-01-agent-production.up.railway.app/?agent=KAIZEN-01';
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || 'claude-haiku-4-5';
const KAIZEN_INTERVAL_MS = parseInt(process.env.KAIZEN_INTERVAL_MS || '28800000');  // 8h
const FIRST_CYCLE_DELAY_MS = parseInt(process.env.FIRST_CYCLE_DELAY_MS || '3600000'); // 1h listening first
const SNAPSHOT_MS = parseInt(process.env.SNAPSHOT_MS || '3600000');                 // hourly trend snapshots

const state = {
  startTime: Date.now(), geckoConnected: false,
  live: {},            // agentId → { lastSeen, stats, alertsInWindow, eventsInWindow }
  history: [],         // hourly snapshots: { t, perAgent: {id: {alerts, events, stats}} } (7 days)
  auditEchoes: [],     // grades/findings heard from AUDITOR on the bus
  vizierEchoes: [],    // verdicts heard from VIZIER
  tasks: [],           // the backlog: {id, target, category, title, rationale, remedy, expectedImpact, priority, effort, status, createdAt, ...}
  taskSeq: 0, cycleCount: 0, alertCount: 0,
  running: false, errors: [], lastError: null,
};

function reportError(msg){ if(state.lastError===msg)return; state.lastError=msg;
  state.errors.push({time:new Date().toISOString(),message:msg}); state.errors=state.errors.slice(-10);
  emit('ERROR','kaizen.error',{message:msg},'HIGH'); }

// Robust JSON extraction — LLMs sometimes append prose after the closing brace
function extractJson(text) {
  const start = text.indexOf('{');
  if (start < 0) throw new Error('no JSON object in response');
  let depth = 0, inStr = false, esc = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (esc) { esc = false; continue; }
    if (ch === '\\') { esc = true; continue; }
    if (ch === '"') inStr = !inStr;
    if (inStr) continue;
    if (ch === '{') depth++;
    if (ch === '}') { depth--; if (depth === 0) return JSON.parse(text.slice(start, i + 1)); }
  }
  throw new Error('unbalanced JSON in response');
}

// ─── Observe: bus ingestion with trend memory ─────────────────────────────────
function ingest(msg) {
  let id = msg.agentId, stats = null;
  if (msg.topic === 'gecko.agent.status' && msg.data?.agentId) { id = msg.data.agentId; stats = msg.data.stats; }
  else if (msg.type === 'STATUS') stats = msg.stats;
  if (!id || id === 'KAIZEN-01') return;
  const a = state.live[id] || { alertsInWindow: 0, eventsInWindow: 0 };
  a.lastSeen = Date.now();
  a.eventsInWindow++;
  if (stats) a.stats = stats;
  if (msg.type === 'ALERT' || /alert/i.test(msg.topic || '')) a.alertsInWindow++;
  state.live[id] = a;
  // Learn from the council: remember AUDITOR grades and VIZIER verdicts
  if (msg.topic === 'audit.report' && msg.data) {
    state.auditEchoes.unshift({ t: new Date().toISOString(), grade: msg.data.grade, findings: (msg.data.findings || []).length });
    state.auditEchoes = state.auditEchoes.slice(0, 20);
  }
  if (msg.topic === 'vizier.memo' && msg.data) {
    state.vizierEchoes.unshift({ t: new Date().toISOString(), asset: msg.data.asset, direction: msg.data.direction,
      verdict: msg.data.verdict, reason: (msg.data.reason || '').slice(0, 300), thesisTag: msg.data.thesisTag || null });
    state.vizierEchoes = state.vizierEchoes.slice(0, 30);
  }
}
setInterval(() => {
  const snap = { t: Date.now(), perAgent: {} };
  Object.entries(state.live).forEach(([id, a]) => {
    snap.perAgent[id] = { alerts: a.alertsInWindow, events: a.eventsInWindow, stats: a.stats || null };
    a.alertsInWindow = 0; a.eventsInWindow = 0;   // window reset
  });
  state.history.push(snap);
  state.history = state.history.slice(-168);      // 7 days of hourly memory
}, SNAPSHOT_MS);

// ─── Propose: the kaizen cycle ────────────────────────────────────────────────
function trendSummary() {
  const byAgent = {};
  state.history.slice(-48).forEach(s => Object.entries(s.perAgent).forEach(([id, m]) => {
    const b = byAgent[id] || { alerts: 0, events: 0, hours: 0 };
    b.alerts += m.alerts; b.events += m.events; b.hours++;
    byAgent[id] = b;
  }));
  return Object.entries(byAgent).map(([id, b]) => ({
    agent: id, hoursObserved: b.hours,
    alertsPerDay: b.hours ? +((b.alerts / b.hours) * 24).toFixed(1) : 0,
    eventsPerHour: b.hours ? +(b.events / b.hours).toFixed(1) : 0,
    latestStats: state.live[id]?.stats || null,
    minutesSinceSeen: state.live[id]?.lastSeen ? Math.round((Date.now() - state.live[id].lastSeen) / 60000) : null,
  }));
}

// Surfaces recurring contrarian-thesis tags across recent Vizier memos so KAIZEN can catch
// "same bet, different ticker" patterns that MATRIX's price-correlation clustering misses.
function thesisClusterSummary() {
  const byTag = {};
  state.vizierEchoes.forEach(e => {
    if (!e.thesisTag) return;
    const b = byTag[e.thesisTag] || { occurrences: 0, assets: [] };
    b.occurrences++;
    if (!b.assets.includes(e.asset)) b.assets.push(e.asset);
    byTag[e.thesisTag] = b;
  });
  return byTag;
}

async function kaizenCycle() {
  if (!ANTHROPIC_API_KEY) { reportError('ANTHROPIC_API_KEY not set — observation-only mode'); return; }
  if (state.running) return;
  state.running = true;
  state.cycleCount++;
  emit('SYS', 'kaizen.cycle.start', { cycle: state.cycleCount, model: ANTHROPIC_MODEL });
  try {
    const openTasks = state.tasks.filter(t => t.status === 'PROPOSED' || t.status === 'ACCEPTED').map(t => ({ id: t.id, target: t.target, title: t.title, status: t.status }));
    const doneRecent = state.tasks.filter(t => t.status === 'DEPLOYED').slice(0, 8).map(t => ({ id: t.id, target: t.target, title: t.title, deployedAt: t.deployedAt, impactNote: t.impactNote || 'pending measurement' }));
    const prompt = `You are KAIZEN-01, the continuous-improvement engine of a 22-agent autonomous crypto trading network (pre-live, paper-trading stage next). Your philosophy: many small, measurable improvements. You PROPOSE; humans deploy.

48-HOUR TREND SUMMARY PER AGENT (from your own hourly memory):
${JSON.stringify(trendSummary(), null, 1)}

COUNCIL ECHOES — recent AUDITOR grades: ${JSON.stringify(state.auditEchoes.slice(0, 5))}
Recent VIZIER memos (asset, direction, verdict, reason, thesisTag): ${JSON.stringify(state.vizierEchoes.slice(0, 12))}
THESIS-CLUSTER COUNTS — same underlying directional bet expressed across DIFFERENT assets, tagged
at execution (occurrences ≥3 across ≥2 assets means the price-correlation cluster cap is being
bypassed by a thesis that isn't price-correlated): ${JSON.stringify(thesisClusterSummary())}

CURRENTLY OPEN TASKS (do NOT duplicate these):
${JSON.stringify(openTasks, null, 1)}

RECENTLY DEPLOYED (assess impact if trends allow):
${JSON.stringify(doneRecent, null, 1)}

If THESIS-CLUSTER COUNTS shows any tag with occurrences ≥3 across ≥2 assets, treat that as strong,
already-evidenced signal worth a task on its own — even if per-agent alert rates look otherwise normal.

Propose 1-4 NEW improvement tasks. Quality over quantity — an empty list is acceptable if nothing genuine emerges. Prefer ENV_VAR remedies (tunable without code). Return ONLY JSON, no fences:
{
 "impactAssessments": [{"taskId": <id>, "assessment": "<did the deployed task measurably help? max 20 words>"}],
 "tasks": [{
   "target": "<agent id or NETWORK>",
   "category": "THRESHOLD_TUNING|DATA_QUALITY|NEW_CAPABILITY|BUG_SUSPECT|PROCESS",
   "title": "<max 12 words>",
   "rationale": "<what the telemetry shows — max 30 words, cite numbers>",
   "remedy": "<EXACT change: env var name + new value, or precise code change description — max 30 words>",
   "expectedImpact": "<measurable outcome — max 15 words>",
   "priority": "P1|P2|P3",
   "effort": "ENV_VAR|SMALL_PATCH|NEW_MODULE"
 }]
}`;
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: ANTHROPIC_MODEL, max_tokens: 1800, messages: [{ role: 'user', content: prompt }] }),
      timeout: 60000,
    });
    if (!res.ok) throw new Error(`Anthropic ${res.status}`);
    const j = await res.json();
    const text = (j.content || []).map(c => c.text || '').join('').trim();
    const out = extractJson(text);
    // Impact assessments close the loop on deployed tasks
    (out.impactAssessments || []).forEach(ia => {
      const t = state.tasks.find(x => x.id === ia.taskId);
      if (t) { t.impactNote = ia.assessment; t.status = 'MEASURED'; emit('SYS', 'kaizen.impact', { id: t.id, target: t.target, assessment: ia.assessment }); }
    });
    // New tasks join the backlog and are DELIVERED on the bus
    (out.tasks || []).slice(0, 4).forEach(nt => {
      state.taskSeq++;
      const task = { id: state.taskSeq, ...nt, status: 'PROPOSED', createdAt: new Date().toISOString() };
      state.tasks.unshift(task);
      emit('TASK', 'kaizen.task', task);
      if (task.priority === 'P1') {
        state.alertCount++;
        emit('ALERT', 'kaizen.alert', { type: 'KAIZEN_P1', asset: task.target, message: task.title, recommendation: task.remedy }, 'HIGH');
      }
    });
    state.tasks = state.tasks.slice(0, 60);
    emit('SYS', 'kaizen.cycle.complete', { cycle: state.cycleCount, newTasks: (out.tasks || []).length, assessed: (out.impactAssessments || []).length, backlog: state.tasks.filter(t => t.status === 'PROPOSED' || t.status === 'ACCEPTED').length });
  } catch (err) { reportError('Cycle: ' + err.message); }
  state.running = false;
}

function setTaskStatus(id, status) {
  const t = state.tasks.find(x => x.id === id);
  if (!t) return null;
  t.status = status;
  if (status === 'DEPLOYED') t.deployedAt = new Date().toISOString();
  emit('TASK', 'kaizen.task.update', { id: t.id, status: t.status, target: t.target, title: t.title });
  return t;
}

// ─── App / Bus / GECKO ────────────────────────────────────────────────────────
const app = express(); app.use(cors()); app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
function broadcast(e){const p=JSON.stringify({...e,agentId:'KAIZEN-01',timestamp:new Date().toISOString()});wss.clients.forEach(c=>{if(c.readyState===WebSocket.OPEN)c.send(p);});}
function emit(type,topic,data,severity='INFO'){broadcast({type,topic,data,severity});console.log(`[${new Date().toISOString()}] [${type}] [${topic}] ${JSON.stringify(data).substring(0,130)}`);}
let geckoWs=null;
function connectGecko(){geckoWs=new WebSocket(GECKO_URL);
  geckoWs.on('open',()=>{state.geckoConnected=true;emit('SYS','kaizen.gecko.connected',{});});
  geckoWs.on('close',()=>{state.geckoConnected=false;setTimeout(connectGecko,5000);});
  geckoWs.on('error',()=>{});
  geckoWs.on('message',raw=>{try{ingest(JSON.parse(raw.toString()));}catch(e){}});}
connectGecko();
setInterval(()=>{if(geckoWs?.readyState===WebSocket.OPEN){geckoWs.send(JSON.stringify({type:'PING'}));
  const open=state.tasks.filter(t=>t.status==='PROPOSED'||t.status==='ACCEPTED').length;
  geckoWs.send(JSON.stringify({type:'STATUS',agentId:'KAIZEN-01',stats:{tasks:state.taskSeq,open,mode:ANTHROPIC_API_KEY?'improving':'no-key'}}));}},15000);
wss.on('connection',ws=>{
  ws.send(JSON.stringify({type:'SYS',topic:'kaizen.handshake',agentId:'KAIZEN-01',timestamp:new Date().toISOString(),
    data:{geckoConnected:state.geckoConnected,scoringEnabled:!!ANTHROPIC_API_KEY,model:ANTHROPIC_MODEL,
      tasks:state.tasks.slice(0,30),agentsHeard:Object.keys(state.live).length,historyHours:state.history.length,
      stats:{uptime:Date.now()-state.startTime,cycles:state.cycleCount,taskTotal:state.taskSeq}}}));
  ws.on('message',raw=>{try{const m=JSON.parse(raw.toString());
    if(m.type==='PING')ws.send(JSON.stringify({type:'PONG',agentId:'KAIZEN-01'}));
    if(m.type==='CYCLE')kaizenCycle();
    if(m.type==='TASK_STATUS'&&m.id&&m.status)setTaskStatus(m.id,m.status);
  }catch(e){}});});
app.get('/health',(_,res)=>res.json({agent:'KAIZEN-01',status:'LIVE',geckoConnected:state.geckoConnected,
  scoringEnabled:!!ANTHROPIC_API_KEY,uptime:Date.now()-state.startTime,cycles:state.cycleCount,
  taskTotal:state.taskSeq,openTasks:state.tasks.filter(t=>t.status==='PROPOSED'||t.status==='ACCEPTED').length,
  agentsHeard:Object.keys(state.live).length,historyHours:state.history.length,errors:state.errors.slice(-3)}));
app.get('/backlog',(_,res)=>res.json({agent:'KAIZEN-01',tasks:state.tasks}));
app.get('/trends',(_,res)=>res.json({agent:'KAIZEN-01',trends:trendSummary(),historyHours:state.history.length}));
app.post('/cycle',(_,res)=>{kaizenCycle();res.json({ok:true});});
app.post('/task/:id/:status',(req,res)=>{
  const t=setTaskStatus(parseInt(req.params.id),req.params.status.toUpperCase());
  if(!t)return res.status(404).json({error:'task not found'});
  res.json(t);});
server.listen(PORT,()=>{
  console.log('\n╔════════════════════════════════════════════════╗');
  console.log('║   KAIZEN-01 — 改善 Continuous Improvement       ║');
  console.log('║   observe → propose → deliver → measure        ║');
  console.log('╚════════════════════════════════════════════════╝\n');
  console.log(`  HTTP →  http://localhost:${PORT}  ·  /backlog /trends`);
  console.log(`  Loop →  every ${KAIZEN_INTERVAL_MS/3600000}h · memory 7 days · ${ANTHROPIC_API_KEY?ANTHROPIC_MODEL:'⚠ NO KEY'}`);
  console.log(`  Law  →  Kaizen proposes, humans deploy\n`);
  setTimeout(kaizenCycle, FIRST_CYCLE_DELAY_MS);
  setInterval(kaizenCycle, KAIZEN_INTERVAL_MS);
});
process.on('SIGTERM',()=>process.exit(0)); process.on('SIGINT',()=>process.exit(0));
