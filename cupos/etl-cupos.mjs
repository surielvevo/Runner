#!/usr/bin/env node
/**
 * scripts/etl-cupos.mjs
 * ETL: UASD API → Supabase (cupos_oficiales + campus_catalogo + refresh búsqueda)
 *
 * Bypass CF:
 *   - Playwright carga la página (CF verifica TLS fingerprint del browser real)
 *   - Los AJAX se hacen desde page.evaluate() con XMLHttpRequest (relativo)
 *     → misma sesión TLS + cookies → CF no re-challenge
 *   - El browser permanece abierto durante todo el ETL
 *
 * El upsert usa onConflict 'periodo,nrc', así que un re-escaneo:
 *   - ACTUALIZA cupos/inscritos/disponibles de secciones ya guardadas
 *   - INSERTA secciones nuevas que la UASD haya abierto en materias existentes
 *
 * MODOS (ETL_MODE) — para no gastar recursos innecesariamente:
 *   full      (default) → re-escanea TODAS las claves. Captura secciones nuevas y
 *                         refresca cupos de todo el catálogo. Úsalo tras meses sin
 *                         correr o cuando la UASD abre la programación de un semestre.
 *   priority            → SOLO re-escanea las claves "calientes": las que tienen
 *                         alguna sección con pocos cupos (disponibles <= HOT_THRESHOLD,
 *                         incluye las llenas en 0 esperando que se libere un cupo),
 *                         más las claves nuevas. Las materias con cupos de sobra NO se
 *                         tocan. El modo más eficiente para correr seguido (cron).
 *                         Se combina con STALE_HOURS para no re-escanear lo recién visto.
 *   stale               → re-escanea SOLO las claves cuya data más reciente tenga más
 *                         de STALE_HOURS horas, MÁS las claves nuevas.
 *   delta               → re-escanea SOLO las claves que aún no existen en la BD.
 *                         El más barato; NO refresca cupos ni capta secciones nuevas
 *                         en materias ya guardadas.
 *
 * USO:  node --env-file=.env.local scripts/etl-cupos.mjs
 *       ETL_MODE=priority HOT_THRESHOLD=10 STALE_HOURS=1 node --env-file=.env.local scripts/etl-cupos.mjs
 */

import fs from 'node:fs';
import { chromium } from 'playwright';
import { createClient } from '@supabase/supabase-js';

// Tolera errores comunes de pegado en el secret: espacios, saltos de línea,
// comillas envolventes y falta de esquema (https://). NO imprime el valor.
function sanitizeUrl(raw) {
  let u = String(raw || '').trim().replace(/^["']+|["']+$/g, '').trim();
  if (u && !/^https?:\/\//i.test(u)) u = 'https://' + u;
  return u.replace(/\/+$/, '');
}

const SUPABASE_URL  = sanitizeUrl(process.env.NEXT_PUBLIC_SUPABASE_URL);
const SERVICE_KEY   = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const ETL_MODE      = (process.env.ETL_MODE || 'full').toLowerCase();
// Turnos paralelos: ETL_SHARDS=3 y ETL_SHARD=0|1|2 (uno por máquina de GitHub).
const SHARD_TOTAL   = Math.max(1, Math.min(8, parseInt(process.env.ETL_SHARDS || '1', 10) || 1));
const SHARD_INDEX   = Math.min(SHARD_TOTAL - 1, Math.max(0, parseInt(process.env.ETL_SHARD || '0', 10) || 0));
function shardDe(clave) {
  let h = 2166136261; // FNV-1a: estable entre corridas y entre máquinas
  for (const ch of String(clave).trim().toUpperCase()) { h ^= ch.charCodeAt(0); h = Math.imul(h, 16777619); }
  return (h >>> 0) % SHARD_TOTAL;
}
const STALE_HOURS   = Number(process.env.STALE_HOURS || 6);
const HOT_THRESHOLD = Number(process.env.HOT_THRESHOLD || 10);
// Claves fijadas por operación: se consultan en cada ciclo priority aunque su
// presupuesto normal todavía no haya vencido. Vacío conserva exactamente el
// comportamiento histórico. Acepta comas/espacios y normaliza LET-1310 → LET1310.
const normalizeClave = (value) => String(value || '').trim().toUpperCase().replace(/-/g, '');
const STATIC_PINNED_KEYS = new Set(
  String(process.env.ETL_PINNED_KEYS || '').split(/[\s,]+/).map(normalizeClave).filter(Boolean)
);
const PINNED_KEYS = new Set(STATIC_PINNED_KEYS);
const HEADLESS      = process.env.HEADLESS !== 'false';
// Concurrencia de fetch DENTRO del navegador. BAJA a propósito: la UASD frena
// (rate-limit) si se le mete ráfaga; ser educado = confiabilidad enterprise.
const FETCH_CONC    = Math.max(1, Number(process.env.SCRAPE_CONCURRENCY || 3));
const BATCH_SIZE    = Math.max(10, Number(process.env.SCRAPE_BATCH || 40));
// Pausa aleatoria por request (ms) para no martillar la UASD.
const GAP_MIN       = Math.max(0, Number(process.env.SCRAPE_GAP_MIN || 400));
const GAP_MAX       = Math.max(GAP_MIN, Number(process.env.SCRAPE_GAP_MAX || 900));
// Cazador aditivo Pro/admin. Corre en OTRA pestaña del mismo navegador para que
// sus fallos y recuperaciones no recarguen la página del ETL general. El intervalo
// es entre INICIOS de consultas: si UASD tarda o frena, el backoff manda.
// APAGADO POR DEFECTO a propósito (incidente 2026-07-24): esta pestaña aparte
// re-navegaba a la UASD y cada recreación era otro challenge de Cloudflare desde
// la misma IP → cadena de bloqueos y 76 min sin datos en pleno pico. Hay que
// pedirlo explícitamente con PRO_HUNTER_ENABLED='true'; así ni un dispatch manual
// ni una variable ausente lo reencienden por accidente. La prioridad de quien
// paga la da `buildPriorityFastLane`, que no añade challenges.
const PRO_HUNTER_ENABLED = process.env.PRO_HUNTER_ENABLED === 'true';
const PRO_HUNTER_INTERVAL_MS = Math.max(5_000, Number(process.env.PRO_HUNTER_INTERVAL_MS || 5_000));
const PRO_HUNTER_KEYS_REFRESH_MS = Math.max(15_000, Number(process.env.PRO_HUNTER_KEYS_REFRESH_MS || 30_000));
const PRO_HUNTER_MAX_BACKOFF_MS = Math.max(
  PRO_HUNTER_INTERVAL_MS,
  Number(process.env.PRO_HUNTER_MAX_BACKOFF_MS || 5 * 60_000)
);
// Multiplicador global de urgencia del modo priority (1 = normal; <1 = más seguido).
const PRIORITY_SPEED = Math.max(0.05, Number(process.env.PRIORITY_SPEED || 1));
const MIN_JITTER    = 500;
const MAX_JITTER    = 1200;
const MAX_RETRIES   = 3;
const XHR_TIMEOUT   = 30_000;
const UASD_PAGE     = 'https://app.uasd.edu.do/ProgramacionPorAsignatura/Default.aspx';
// PRESUPUESTO GLOBAL de tiempo (min). El run PARA solo, en orden, antes de que
// el timeout del job lo mate: como la cola va ordenada por urgencia, lo crítico
// ya quedó al día y el resto entra en el próximo ciclo. 0 = sin límite.
// Evita el peor modo de fallo observado: un run colgado 6h tapona el grupo de
// concurrencia y nadie scrapea nada en todo ese tiempo.
const DEADLINE_MIN  = Math.max(0, Number(process.env.ETL_DEADLINE_MIN || 0));
const START_TS      = Date.now();
// Un lote jamás puede tardar más que esto: si page.evaluate se cuelga (browser
// zombie), lo cortamos y relanzamos Chromium en vez de esperar horas.
const BATCH_HARD_TIMEOUT = 15 * 60_000;
const RUNNER_NAME   = String(process.env.RUNNER_NAME || process.env.COMPUTERNAME || '').trim();
// Perfil PERSISTENTE de Chromium: la cookie cf_clearance sobrevive entre
// corridas → el supervisor de cada 5 min reutiliza el pase de Cloudflare en
// vez de re-resolver el challenge desde cero (re-desafiar en ráfaga es lo que
// castiga la reputación de la IP). Vacío = perfil efímero (comportamiento viejo).
const PROFILE_DIR   = String(process.env.CHROME_PROFILE_DIR || '').trim();
// true = saltarse el enfriamiento anti-bloqueo (solo para corridas manuales).
const IGNORE_COOLDOWN = process.env.ETL_IGNORE_COOLDOWN === 'true';

// (2026-11-10, Runner) PUENTE sin claves: en GitHub Actions el ETL no lleva la clave de
// servicio. Se identifica con el token OIDC que GitHub firma para cada ejecución y la
// función `etl-puente` de Supabase verifica esa firma (solo este repo, rama main, este
// workflow) y escribe con la clave que vive allí. Con clave local, el camino de siempre.
const PUENTE = sanitizeUrl(process.env.ETL_PUENTE);
if (!(SUPABASE_URL && SERVICE_KEY) && !PUENTE) {
  console.error('❌  Faltan NEXT_PUBLIC_SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY (o ETL_PUENTE en GitHub Actions)');
  process.exit(1);
}
let _oidc = { valor: null, vence: 0 };
async function tokenGitHub() {
  if (_oidc.valor && Date.now() < _oidc.vence - 90_000) return _oidc.valor;
  const u = process.env.ACTIONS_ID_TOKEN_REQUEST_URL, t = process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN;
  if (!u || !t) throw new Error('Sin token OIDC de GitHub (el workflow necesita permissions: id-token: write)');
  const r = await fetch(`${u}&audience=cupopino-etl`, { headers: { Authorization: `Bearer ${t}` } });
  const { value } = await r.json();
  const exp = JSON.parse(Buffer.from(String(value).split('.')[1], 'base64url').toString()).exp * 1000;
  _oidc = { valor: value, vence: exp };
  return value;
}
async function fetchPuente(input, init = {}) {
  const h = new Headers(init.headers || (input instanceof Request ? input.headers : undefined));
  h.set('x-gh-oidc', await tokenGitHub());
  return fetch(input, { ...init, headers: h });
}

if (!['full', 'priority', 'stale', 'delta'].includes(ETL_MODE)) {
  console.error(`❌  ETL_MODE inválido: "${ETL_MODE}". Usa full | priority | stale | delta.`);
  process.exit(1);
}

const supabase = SUPABASE_URL && SERVICE_KEY
  ? createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } })
  : createClient(PUENTE, 'puente-oidc', { auth: { persistSession: false }, global: { fetch: fetchPuente } });

function jitter() { return MIN_JITTER + Math.random() * (MAX_JITTER - MIN_JITTER); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function minutesLeft() { return DEADLINE_MIN ? DEADLINE_MIN - (Date.now() - START_TS) / 60_000 : Infinity; }

// ── Claves vigiladas DINÁMICAS (proyección del equipo) ───────────────────────
// Además de ETL_PINNED_KEYS (env, p.ej. LET1310), el panel de admin puede marcar
// cuentas cuya proyección se vigila con prioridad (tabla equipo_cupos_vigilancia
// → RPC etl_claves_vigiladas). Sus materias se AÑADEN al set de claves fijadas
// para entrar en cada ciclo priority y captar aperturas casi en vivo.
//
// 100% ADITIVO y a prueba de fallos: si la RPC no existe todavía, falla o
// devuelve vacío, PINNED_KEYS queda EXACTAMENTE como el env → conducta histórica
// intacta. Solo puede AÑADIR claves a vigilar (más frescura); jamás quita ni
// altera nada de lo que ya se scrapea o se muestra.
async function loadDynamicPinnedKeys() {
  try {
    const { data, error } = await supabase.rpc('etl_claves_vigiladas');
    if (error || !Array.isArray(data)) return;
    let add = 0;
    for (const row of data) {
      const k = normalizeClave(row && typeof row === 'object' ? row.clave : row);
      if (k && !PINNED_KEYS.has(k)) { PINNED_KEYS.add(k); add++; }
    }
    if (add) console.log(`   Vigilancia dinámica del equipo: +${add} clave(s) fijada(s) desde su proyección.`);
  } catch (e) {
    console.warn('   ⚠ vigilancia dinámica no disponible (se sigue con las claves del env):', e?.message || e);
  }
}

// ── Carril de DEMANDA (2026-11-11) ────────────────────────────────────────────
// Lo que un estudiante abre en la programación de CupoPino con datos de más de 2 min
// queda pedido en `cupos_demanda` (como mucho una vez por minuto y materia, para
// todos). Cada turno lo lee como mucho cada DEMANDA_CADA_MS (1 RPC diminuta por el
// puente) y mete las de SU parte del catálogo justo después del lote en curso, o lo
// despierta del reposo. NO sube el ritmo de consultas a la UASD: reordena el mismo
// caudal, igual que las claves vigiladas. Tope por vuelta para que una avalancha de
// pedidos nunca deje sin turno al resto del catálogo. Si la RPC falla: cero efecto.
const DEMANDA_CADA_MS = Math.max(15_000, Number(process.env.DEMANDA_CADA_MS || 30_000));
const DEMANDA_TOPE = Math.max(1, Math.min(20, Number(process.env.DEMANDA_TOPE || 6)));
let demandaLeidaEn = 0;
const demandaAtendida = new Map(); // clave del catálogo → cuándo entró a la cola (no repetir en 60 s)
const colaDemanda = [];
async function revisarDemanda(clavePorNorm, forzar = false) {
  if (ETL_MODE !== 'priority' || !clavePorNorm) return colaDemanda.length;
  if (!forzar && Date.now() - demandaLeidaEn < DEMANDA_CADA_MS) return colaDemanda.length;
  demandaLeidaEn = Date.now();
  try {
    const { data, error } = await withTimeout(supabase.rpc('etl_demanda'), 8_000, 'DEMANDA_TIMEOUT');
    if (error || !Array.isArray(data)) return colaDemanda.length;
    const ahora = Date.now();
    for (const row of data) {
      if (colaDemanda.length >= DEMANDA_TOPE) break;
      const clave = clavePorNorm.get(normalizeClave(row && typeof row === 'object' ? row.clave : row));
      if (!clave) continue; // no es de este turno (o ya no está en el universo)
      if (ahora - (demandaAtendida.get(clave) || 0) < 60_000) continue;
      demandaAtendida.set(clave, ahora);
      colaDemanda.push(clave);
    }
  } catch { /* sin demanda: la cola normal sigue igual */ }
  return colaDemanda.length;
}
function tomarDemanda() { return colaDemanda.splice(0); }

// Lista AUTORITATIVA del carril de 5 s. A diferencia de PINNED_KEYS, esta lista
// se reemplaza en cada lectura: si un Pro desactiva/consigue una materia deja de
// consumir el worker sin esperar al próximo job. etl_claves_vigiladas devuelve
// únicamente vigilancia administrativa + objetivos Pro activos; Free no entra.
let proHunterKeys = [...STATIC_PINNED_KEYS];
let proHunterKeysFetchedAt = 0;

async function refreshProHunterKeys(force = false) {
  if (!force && Date.now() - proHunterKeysFetchedAt < PRO_HUNTER_KEYS_REFRESH_MS) {
    return proHunterKeys;
  }
  try {
    const { data, error } = await withTimeout(
      supabase.rpc('etl_claves_vigiladas'),
      10_000,
      'PRO_KEYS_TIMEOUT'
    );
    if (error || !Array.isArray(data)) throw error || new Error('respuesta inválida');
    const next = new Set(STATIC_PINNED_KEYS);
    for (const row of data) {
      const key = normalizeClave(row && typeof row === 'object' ? row.clave : row);
      if (key) next.add(key);
    }
    proHunterKeys = [...next];
    proHunterKeysFetchedAt = Date.now();
    return proHunterKeys;
  } catch (e) {
    // Conservar el último set sano. Un fallo de esta RPC jamás toca el ETL normal.
    console.warn('   ⚠ [PRO] no se pudo refrescar la lista; conserva la última:', e?.message || e);
    proHunterKeysFetchedAt = Date.now();
    return proHunterKeys;
  }
}

// ── IP pública de salida (para el circuit breaker IP-aware) ───────────────────
// Consulta servicios que NO están detrás del Cloudflare de la UASD, así que se
// pueden alcanzar aunque la UASD nos bloquee. Con esto el scraper SABE cuándo la
// IP cambió (hotspot/router nuevo) → se lanza de una; y deja de martillar una IP
// muerta que solo alarga el castigo de Cloudflare. Best-effort: si falla, null.
let _cachedIp = undefined;
async function getPublicIP() {
  if (_cachedIp !== undefined) return _cachedIp;
  const sources = ['https://api.ipify.org', 'https://ifconfig.me/ip', 'https://icanhazip.com'];
  for (const url of sources) {
    try {
      const ctrl = new AbortController();
      const to = setTimeout(() => ctrl.abort(), 8000);
      const res = await fetch(url, { signal: ctrl.signal });
      clearTimeout(to);
      if (!res.ok) continue;
      const ip = (await res.text()).trim();
      if (/^\d{1,3}(\.\d{1,3}){3}$|:/.test(ip)) { _cachedIp = ip; return ip; }
    } catch { /* siguiente fuente */ }
  }
  _cachedIp = null;
  return null;
}

// Corta una promesa que nunca resuelve (p.ej. page.evaluate sobre un browser
// muerto). Sin esto, un solo cuelgue consumía el timeout completo del job.
function withTimeout(promise, ms, label) {
  let t;
  const timer = new Promise((_, rej) => { t = setTimeout(() => rej(new Error(label)), ms); });
  return Promise.race([promise, timer]).finally(() => clearTimeout(t));
}

// ── Heartbeat (etl_runs): latido para el watchdog externo ─────────────────────
// TODO el heartbeat es best-effort: la telemetría JAMÁS tumba el scraping.
let hbId = null, hbBroken = false;
async function hbStart(extra = {}) {
  try {
    const ip = await getPublicIP();
    const { data, error } = await supabase
      .from('etl_runs')
      .insert({ mode: ETL_MODE, runner: RUNNER_NAME || null, status: 'running', public_ip: ip, ...extra })
      .select('id')
      .single();
    if (error) throw error;
    hbId = data.id;
    // Retención: corridas de más de 30 días se limpian solas.
    const cutoff = new Date(Date.now() - 30 * 24 * 3600_000).toISOString();
    await supabase.from('etl_runs').delete().lt('started_at', cutoff);
    // Higiene: filas 'running' huérfanas (el runner murió a mitad de corrida
    // sin cerrar el latido) se marcan como error para que el historial sea veraz.
    const staleBeat = new Date(Date.now() - 2 * 3600_000).toISOString();
    await supabase.from('etl_runs')
      .update({ status: 'error', finished_at: new Date().toISOString(), detail: 'huérfana: el proceso murió sin cerrar (runner caído a mitad de corrida)' })
      .eq('status', 'running').lt('last_beat', staleBeat).neq('id', hbId);
  } catch (e) {
    hbBroken = true;
    console.warn('   ⚠ heartbeat no disponible (etl_runs):', e?.message || e);
  }
}
async function hbBeat(patch = {}) {
  if (hbId == null || hbBroken) return;
  try {
    await supabase.from('etl_runs')
      .update({ last_beat: new Date().toISOString(), ...patch })
      .eq('id', hbId);
  } catch { /* nunca interrumpir el ETL por telemetría */ }
}
async function hbEnd(status, patch = {}) {
  if (hbId == null || hbBroken) return;
  try {
    await supabase.from('etl_runs')
      .update({ status, finished_at: new Date().toISOString(), last_beat: new Date().toISOString(), ...patch })
      .eq('id', hbId);
  } catch { /* idem */ }
}

async function pLimit(tasks, n) {
  let i = 0;
  const results = [];
  async function worker() {
    while (i < tasks.length) { const idx = i++; results[idx] = await tasks[idx](); }
  }
  await Promise.all(Array.from({ length: Math.min(n, tasks.length) }, worker));
  return results;
}

// ── Abrir página y esperar que Cloudflare emita cf_clearance ───────────────────
// El "managed challenge" de CF lo resuelve un navegador REAL (headful) ejecutando
// su JS; al pasarlo, CF entrega la cookie cf_clearance, que es httpOnly y por eso
// SOLO se ve con context.cookies() (no con document.cookie). Reintenta recargando
// hasta que aparezca la cookie o el título deje de ser de challenge.
function looksBlocked(t) {
  const s = (t || '').toLowerCase();
  // OJO: CF localiza el título del reto al idioma del navegador. En la PC Windows
  // (locale es-DO) el interstitial "Just a moment…" sale como "Un momento…", que
  // ANTES no se detectaba → el script creía haber pasado CF y todas las XHR daban
  // 403. Incluimos las variantes ES/EN del reto.
  return s === '' ||
    s.includes('just a moment') || s.includes('un momento') || s.includes('momento') ||
    s.includes('attention') || s.includes('cloudflare');
}

async function hasClearance(context) {
  // SOLO cf_clearance indica challenge superado. __cf_bm lo pone CF en CUALQUIER
  // respuesta (incluida la página de bloqueo), así que no sirve como señal.
  const cookies = await context.cookies().catch(() => []);
  return cookies.some(c => /^cf_clearance$/i.test(c.name));
}

// Bloqueo DURO de Cloudflare (WAF/1020): la IP está en penalización temporal.
// NO se resuelve recargando; martillar solo alarga el castigo → hay que rendirse.
function isHardBlock(t) {
  const s = (t || '').toLowerCase();
  return s.includes('access denied') || s.includes('acceso denegado') ||
         s.includes('restrict access') || s.includes('1020');
}

// Si CF pide un Turnstile INTERACTIVO ("Verifica que eres un humano" con
// checkbox), un navegador que solo espera nunca pasa. Intentamos el clic como
// lo haría una persona: primero el checkbox dentro del iframe del widget, y si
// no, un clic por coordenadas sobre el iframe. Best-effort: jamás lanza.
async function tryClickTurnstile(page) {
  try {
    for (const frame of page.frames()) {
      if (/challenges\.cloudflare\.com|turnstile/i.test(frame.url() || '')) {
        const cb = frame.locator('input[type="checkbox"]');
        if (await cb.count().catch(() => 0)) {
          await cb.first().click({ timeout: 3000, force: true }).catch(() => {});
          return true;
        }
      }
    }
    const el = await page.$('iframe[src*="challenges.cloudflare.com"], iframe[title*="challenge" i], iframe[title*="Cloudflare" i]');
    if (el) {
      const box = await el.boundingBox();
      if (box) { await page.mouse.click(box.x + 30, box.y + box.height / 2); return true; }
    }
  } catch { /* best-effort */ }
  return false;
}

async function openPage(context, maxAttempts = 3) {
  const page = await context.newPage();
  let lastTitle = '';
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await page.goto(UASD_PAGE, { waitUntil: 'domcontentloaded', timeout: 90_000 });
      // Humanización ligera: CF puntúa señales de interacción real.
      await page.mouse.move(200 + Math.random() * 500, 150 + Math.random() * 300).catch(() => {});

      // Espera PACIENTE (hasta ~55 s): el managed challenge tarda unos segundos;
      // el interactivo necesita nuestros clics al Turnstile (cada ~8 s, máx 4).
      const waitUntil = Date.now() + 55_000;
      let clicks = 0, lastClick = 0;
      while (Date.now() < waitUntil) {
        await sleep(2500);
        lastTitle = await page.title().catch(() => '');
        if (!looksBlocked(lastTitle)) break;
        if (isHardBlock(lastTitle)) throw new Error(`IP_BLOQUEADA::${lastTitle}`);
        if (clicks < 4 && Date.now() - lastClick > 8000) {
          if (await tryClickTurnstile(page)) console.log('   ↳ clic al widget de verificación…');
          clicks++; lastClick = Date.now();
        }
        await page.mouse.move(150 + Math.random() * 600, 120 + Math.random() * 350).catch(() => {});
      }

      // A veces la clearance llega pero la pestaña quedó en el interstitial:
      // una recarga con cookie válida ya entra directo.
      if (looksBlocked(lastTitle) && await hasClearance(context)) {
        await page.reload({ waitUntil: 'domcontentloaded', timeout: 45_000 }).catch(() => {});
        await sleep(2500);
        lastTitle = await page.title().catch(() => '');
      }

      // Éxito REAL: el título ya NO es página de bloqueo/challenge de Cloudflare.
      if (!looksBlocked(lastTitle)) {
        console.log(`   ✓ CF superado (intento ${attempt}/${maxAttempts}) · título="${lastTitle}"`);
        return page;
      }
      console.warn(`   ⚠ CF challenge sin resolver (intento ${attempt}/${maxAttempts}) · título="${lastTitle}".`);
    } catch (e) {
      if (String(e.message).startsWith('IP_BLOQUEADA::')) throw e; // no seguir tocando la IP
      console.warn(`   ⚠ openPage intento ${attempt}/${maxAttempts}: ${e.message}`);
    }
    // Espera LARGA entre intentos para no reforzar el rate-limit/castigo de CF.
    await sleep(20_000 * attempt + jitter());
  }
  throw new Error(
    `No se pudo pasar Cloudflare (título="${lastTitle}"). Probable rate-limit/penalización ` +
    `temporal de la IP por exceso de peticiones. El sistema esperará al próximo ciclo.`
  );
}

// ── XHR desde page.evaluate (mismo TLS/sesión que el browser) ────────────────
async function pageXHR(page, method, body = {}, attempt = 1) {
  try {
    const result = await page.evaluate(
      ({ method, bodyStr, timeout }) => new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', 'Default.aspx/' + method, true);
        xhr.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
        xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
        xhr.setRequestHeader('Accept', 'application/json, text/javascript, */*; q=0.01');
        xhr.timeout = timeout;
        xhr.ontimeout = () => reject(new Error('XHR timeout'));
        xhr.onerror   = () => reject(new Error('XHR network error'));
        xhr.onload    = () => {
          if (xhr.status === 200 && !xhr.responseText.trimStart().startsWith('<')) {
            resolve(xhr.responseText);
          } else {
            reject(new Error('HTTP ' + xhr.status + ' isHTML=' + xhr.responseText.trimStart().startsWith('<')));
          }
        };
        xhr.send(bodyStr);
      }),
      { method, bodyStr: JSON.stringify(body), timeout: XHR_TIMEOUT }
    );
    return JSON.parse(JSON.parse(result).d);
  } catch (err) {
    if (attempt >= MAX_RETRIES) throw err;
    // Si CF devolvió HTML/403, la sesión perdió clearance: recargamos la pestaña
    // para re-resolver el challenge antes de reintentar la llamada.
    if (/isHTML=true|HTTP 403/i.test(err.message)) {
      try {
        await page.reload({ waitUntil: 'domcontentloaded', timeout: 45_000 });
        await sleep(2500);
      } catch { /* el backoff de abajo cubre el caso */ }
    }
    const delay = 1000 * Math.pow(2, attempt - 1) + jitter();
    console.warn(`   ⚠  ${method} retry ${attempt}/${MAX_RETRIES}: ${err.message} → ${(delay/1000).toFixed(1)}s`);
    await sleep(delay);
    return pageXHR(page, method, body, attempt + 1);
  }
}

// ── Re-resolver Cloudflare sobre una página existente (recarga hasta pasar) ────
async function recoverPage(page) {
  for (let a = 1; a <= 3; a++) {
    try {
      await page.goto(UASD_PAGE, { waitUntil: 'domcontentloaded', timeout: 90_000 });
      await sleep(2500);
      const t = await page.title().catch(() => '');
      if (!looksBlocked(t)) return true;
    } catch { /* reintenta */ }
    await sleep(2000 * a + jitter());
  }
  return false;
}

// ── Descarga getData de un LOTE de claves, con concurrencia y BACKOFF adaptativo ─
// Una sola pestaña ya con cf_clearance (liviano para el Celeron). Cada request se
// reintenta si la UASD nos frena (429/503/500) con espera exponencial DENTRO del
// navegador, para no fallar por rate-limit. Devuelve por clave el resultado tipado:
//   { clave, ok, rows }                    → datos OK
//   { clave, ok:false, blocked:true }      → Cloudflare re-desafió (HTML/403)
//   { clave, ok:false, rateLimited:true }  → nos frenaron incluso tras reintentos
//   { clave, ok:false, status/err }        → otro fallo
async function fetchBatch(page, claves, opts) {
  return await page.evaluate(async ({ claves, conc, gapMin, gapMax, timeout, tries }) => {
    const out = [];
    const nap = (ms) => new Promise(r => setTimeout(r, ms));
    let i = 0;

    async function fetchOne(clave) {
      for (let attempt = 1; attempt <= tries; attempt++) {
        await nap(gapMin + Math.random() * (gapMax - gapMin));
        try {
          const ctrl = new AbortController();
          const to = setTimeout(() => ctrl.abort(), timeout);
          const res = await fetch('Default.aspx/getData', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json; charset=UTF-8',
              'X-Requested-With': 'XMLHttpRequest',
              'Accept': 'application/json, text/javascript, */*; q=0.01',
            },
            body: JSON.stringify({ campus: '000', clave }),
            signal: ctrl.signal,
          });
          clearTimeout(to);
          const text = await res.text();
          const isHtml = text.trimStart().startsWith('<');

          if (res.status === 200 && !isHtml) {
            return { clave, ok: true, rows: JSON.parse(JSON.parse(text).d) };
          }
          // Nos frenaron (rate-limit / servidor ocupado) → esperar y reintentar.
          if (res.status === 429 || res.status === 503 || res.status === 502 || res.status === 500) {
            if (attempt < tries) { await nap(1500 * Math.pow(2, attempt) + Math.random() * 1000); continue; }
            return { clave, ok: false, rateLimited: true, status: res.status };
          }
          // Cloudflare re-desafió (página HTML) o 403 → no seguir martillando.
          if (isHtml || res.status === 403) return { clave, ok: false, blocked: true, status: res.status };
          // Otro código: un reintento suave.
          if (attempt < tries) { await nap(800 * attempt); continue; }
          return { clave, ok: false, status: res.status };
        } catch (e) {
          if (attempt < tries) { await nap(800 * attempt); continue; }
          return { clave, ok: false, err: String((e && e.message) || e) };
        }
      }
    }

    async function worker() {
      while (i < claves.length) { out.push(await fetchOne(claves[i++])); }
    }
    await Promise.all(Array.from({ length: Math.min(conc, claves.length) }, worker));
    return out;
  }, {
    claves,
    conc: opts.conc, gapMin: opts.gapMin, gapMax: opts.gapMax,
    timeout: XHR_TIMEOUT, tries: opts.tries,
  });
}

// ── Row mapper ─────────────────────────────────────────────────────────────────
function mapRow(r) {
  if (!r || typeof r !== 'object') r = {};  // la UASD a veces mete un null en el arreglo
  return {
    periodo:     String(r.periodo ?? '').trim(),
    nrc:         String(r.nrc ?? '').trim(),
    clave:       String(r.clave ?? '').trim(),
    asignatura:  String(r.asignatura ?? '').trim(),
    seccion:     String(r.seccion ?? '').trim(),
    tipo:        String(r.tipo ?? '').trim(),
    campus:      String(r.campus ?? '').trim(),
    horario:     String(r.horario ?? '').trim(),
    dias:        String(r.dias ?? '').trim(),
    aula:        String(r.aula ?? '').trim(),
    edificio:    String(r.edificio ?? '').trim(),
    modalidad:   String(r.modalidad ?? '').trim(),
    cupos:       parseInt(r.cupos ?? '0', 10) || 0,
    inscritos:   parseInt(r.inscritos ?? '0', 10) || 0,
    disponibles: parseInt(r.disponibles ?? '0', 10) || 0,
    id_profesor: r.id_profesor != null ? String(r.id_profesor).trim() : null,
    profesor:    r.nombre != null ? String(r.nombre).trim() : null,
    scraped_at:  new Date().toISOString(),
  };
}

async function upsertRows(rows) {
  if (!rows.length) return;
  // De-duplicar por la MISMA clave de conflicto (periodo,nrc) antes de mandar. La
  // API de la UASD a veces devuelve la misma sección (NRC) dos veces; si dos filas
  // con igual (periodo,nrc) caen en un mismo upsert, Postgres tira
  // "ON CONFLICT DO UPDATE command cannot affect row a second time" y RECHAZA EL
  // LOTE COMPLETO (hasta 500 filas), así que esas materias no se actualizaban aunque
  // el job saliera verde. Nos quedamos con la última ocurrencia (dato más reciente).
  const byKey = new Map();
  for (const r of rows) byKey.set(`${r.periodo}|${r.nrc}`, r);
  const deduped = [...byKey.values()];

  const BATCH = 500;
  for (let i = 0; i < deduped.length; i += BATCH) {
    const { error } = await supabase
      .from('cupos_oficiales')
      .upsert(deduped.slice(i, i + BATCH), { onConflict: 'periodo,nrc', ignoreDuplicates: false });
    if (error) console.error('   ❌ upsert:', error.message);
  }
}

// ── Reconciliación: CIERRA las secciones que la UASD dejó de listar ─────────────
// Cuando una sección se LLENA, la UASD la QUITA de getData (no la devuelve con 0),
// así que el upsert nunca la baja y la fila quedaba mostrando "cupos" fantasma para
// siempre. Los estudiantes lo reportaban: "aparece con cupo disponible y cuando
// coloco el código está llena". Aquí, SOLO para claves que se scrapearon OK y
// devolvieron secciones, cualquier NRC que la BD tenga con disponibles>0 y que YA
// NO venga en el scrape = llena → disponibles=0, inscritos=cupos.
//   · SEGURO: si el scrape de una clave falló o vino vacío, esa clave NI ENTRA en
//     `scrapedByClave` (main solo la agrega con secciones OK) → un bloqueo de CF
//     nunca puede vaciar una materia.
//   · NO borra la fila: la página SEO de la sección sigue viva (regla 2) y el
//     planificador la re-chequea cada ~15 min (presupuesto minDisp<=0) para cazar
//     cupos liberados —justo el comportamiento que refreshBudgetHours ya esperaba—.
//   · EFICIENTE (regla 6): 1 SELECT por lote que trae SOLO las secciones que hoy
//     muestran cupo (las de riesgo), y un upsert únicamente de las que hay que
//     cerrar (normalmente pocas). Auto-sana: si la UASD la omitió por un glitch,
//     el próximo ciclo la vuelve a listar y la reabre.
async function cerrarSeccionesDesaparecidas(scrapedByClave) {
  const claves = [...scrapedByClave.keys()];
  if (!claves.length) return 0;

  const vivas = [];
  const IN = 200;
  for (let i = 0; i < claves.length; i += IN) {
    const { data, error } = await supabase
      .from('cupos_oficiales')
      .select('periodo, nrc, clave, cupos')
      .in('clave', claves.slice(i, i + IN))
      .gt('disponibles', 0);
    if (error) { console.warn('   ⚠ reconciliación SELECT:', error.message); continue; }
    if (Array.isArray(data)) vivas.push(...data);
  }

  const now = new Date().toISOString();
  const cerrar = [];
  for (const v of vivas) {
    const set = scrapedByClave.get(String(v.clave || '').toUpperCase());
    if (set && !set.has(String(v.nrc || '').trim())) {
      const cupos = parseInt(v.cupos ?? '0', 10) || 0;
      // Solo las columnas de cupo → el UPDATE del upsert conserva horario, profesor,
      // asignatura, etc. (estas filas YA existen, así que siempre entra por UPDATE).
      cerrar.push({ periodo: v.periodo, nrc: v.nrc, clave: v.clave, cupos, inscritos: cupos, disponibles: 0, scraped_at: now });
    }
  }
  if (!cerrar.length) return 0;

  const BATCH = 500;
  for (let i = 0; i < cerrar.length; i += BATCH) {
    const { error } = await supabase
      .from('cupos_oficiales')
      .upsert(cerrar.slice(i, i + BATCH), { onConflict: 'periodo,nrc', ignoreDuplicates: false });
    if (error) console.warn('   ⚠ reconciliación upsert:', error.message);
  }
  return cerrar.length;
}

// ── Estado por clave: { minDisp, ageH, isDue } ────────────────────────────────
// EFICIENTE: llama a la función SQL claves_estado() que AGREGA en Postgres y
// devuelve 1 fila por clave (~1.9k) en vez de las ~21k secciones → egress mínimo
// (clave para no gastar recursos de Supabase). Si la función no existe todavía,
// cae a leer y agregar en JS.
async function getClaveState() {
  const map = new Map(); // clave (upper) → { minDisp, ageH, isDue }

  // PostgREST corta cada respuesta a ~1000 filas → paginamos con range para traer
  // TODAS las ~1.9k claves (si no, el planificador cree que faltan y scrapea de más).
  let rpcOk = true, rpcFrom = 0, rpcErr = null; const page = 1000;
  for (let guard = 0; guard < 50; guard++) {
    const { data, error } = await supabase
      .rpc('claves_estado', { speed: PRIORITY_SPEED })
      .range(rpcFrom, rpcFrom + page - 1);
    if (error) { rpcOk = (rpcFrom > 0); rpcErr = error; break; }   // si falla en la 1a página, usamos fallback
    if (!Array.isArray(data) || data.length === 0) break;
    for (const r of data) {
      const clave = String(r.clave || '').trim().toUpperCase();
      if (!clave) continue;
      map.set(clave, {
        minDisp: parseInt(r.min_disp ?? '0', 10) || 0,
        ageH: Number(r.age_hours) || 0,
        isDue: !!r.is_due,
        nSecc: parseInt(r.n_secciones ?? '0', 10) || 0,
      });
    }
    if (data.length < page) break;
    rpcFrom += page;
  }
  if (rpcOk && map.size > 0) return map;
  map.clear();

  // Fallback: agregación en JS (si la RPC no está disponible).
  console.warn('   ⚠ RPC claves_estado no disponible; leyendo directo:', rpcErr?.message || '');
  const raw = new Map();
  let from = 0; const batch = 1000;
  for (let guard = 0; guard < 200; guard++) {
    const { data: d, error: e } = await supabase
      .from('cupos_oficiales').select('clave, scraped_at, disponibles').range(from, from + batch - 1);
    if (e || !d || d.length === 0) break;
    for (const r of d) {
      const clave = String(r.clave || '').trim().toUpperCase();
      if (!clave) continue;
      const ts = r.scraped_at ? Date.parse(r.scraped_at) : 0;
      const disp = parseInt(r.disponibles ?? '0', 10) || 0;
      const prev = raw.get(clave);
      if (!prev) raw.set(clave, { latest: ts, minDisp: disp, nSecc: 1 });
      else { if (ts > prev.latest) prev.latest = ts; if (disp < prev.minDisp) prev.minDisp = disp; prev.nSecc++; }
    }
    if (d.length < batch) break;
    from += batch;
  }
  const now = Date.now();
  for (const [clave, v] of raw) {
    const ageH = (now - v.latest) / 3600_000;
    map.set(clave, { minDisp: v.minDisp, ageH, isDue: ageH >= refreshBudgetHours(v.minDisp), nSecc: v.nSecc });
  }
  return map;
}

// ── Algoritmo INTELIGENTE de urgencia ─────────────────────────────────────────
// "Presupuesto de frescura" por clave según sus cupos MÁS bajos (minDisp): mientras
// menos cupos disponibles (más cerca de llenarse), más seguido se refresca. Las de
// cupos de sobra caen al fondo de la cola (presupuesto largo). Así el sistema
// concentra el esfuerzo en las secciones que "van bajando".
// Presupuestos AFINADOS PARA PICO DE SELECCIÓN. Lo que manda no es el valor
// absoluto sino el RATIO entre niveles: mientras más apretado el cupo, mucho más
// corto el presupuesto → esa clave "vence" antes y burbujea al frente de cada
// pasada del bucle de re-priorización, refrescándose tan seguido como el caudal
// (~15 claves/min) lo permite. Así lo más crítico (1-3 cupos) baja de ~52 min a
// ~3-6 min sin subir la tasa de peticiones (mismo caudal, solo reparte más hacia
// lo crítico). DEBE COINCIDIR con la función SQL claves_estado (migración
// 20260722_claves_estado_pico.sql), que decide is_due.
function refreshBudgetHours(minDisp) {
  let base;
  if (minDisp <= 0)       base = 0.10;   // llena: caza de cupos liberados → ~6 min
  else if (minDisp <= 3)  base = 0.05;   // crítico (se está llenando) → ~3 min (máxima prioridad)
  else if (minDisp <= 5)  base = 0.08;   // muy bajo (llenándose) → ~5 min, por encima de las llenas
  else if (minDisp <= 10) base = 0.20;   // caliente → ~12 min
  else if (minDisp <= 15) base = 0.5;    // templada → 30 min
  else if (minDisp <= 24) base = 1.5;    // bajando → 1.5 h
  else                    base = 8;      // 25+ cupos: barrido ligero cada 8h (cazar secciones nuevas)
  return base * PRIORITY_SPEED;
}
// Mayor score = más ATRASADA = va PRIMERO en la cola (sort descendente).
// Prioridad = qué tan atrasada está respecto a su presupuesto (age/budget). Así
// nada se queda congelado: las muy viejas (aunque tengan cupos de sobra) suben, y
// las de pocos cupos igual suben rápido (su presupuesto es corto). Nuevas = tope.
function overdueScore(st) {
  if (!st) return Number.POSITIVE_INFINITY;              // nueva → máxima prioridad
  const budget = Math.max(refreshBudgetHours(st.minDisp), 0.05);
  return st.ageH / budget;
}

// ── Selección + ORDEN de claves a extraer según ETL_MODE ──────────────────────
function selectTargets(claves, state) {
  console.log(`   En BD: ${state.size} claves con data previa.`);
  // Ordena SIEMPRE por atraso: lo más desactualizado primero (nada se queda
  // congelado). Si un run se corta o nos frenan, lo más viejo ya quedó al día.
  const byUrgency = (a, b) => overdueScore(state.get(b.toUpperCase())) - overdueScore(state.get(a.toUpperCase()));

  if (ETL_MODE === 'delta') {
    const targets = claves.filter(c => !state.has(c.toUpperCase()));
    console.log(`   Modo delta → ${targets.length} claves nuevas.`);
    return targets;
  }

  if (ETL_MODE === 'full') {
    const targets = [...claves].sort(byUrgency);
    console.log(`   Modo full → ${targets.length} claves (orden inteligente: menos cupos primero).`);
    return targets;
  }

  if (ETL_MODE === 'stale') {
    const targets = claves.filter(c => { const st = state.get(c.toUpperCase()); return !st || st.ageH >= STALE_HOURS; }).sort(byUrgency);
    console.log(`   Modo stale (>${STALE_HOURS}h) → ${targets.length} claves.`);
    return targets;
  }

  // priority INTELIGENTE: cada clave vence según su presupuesto (a menos cupos,
  // más seguido; is_due lo calcula la función SQL). Las de cupos de sobra rara vez
  // entran → cola de último. Las nuevas (aún no en BD) siempre entran.
  let nuevas = 0; const t = { fijas: 0, criticas: 0, calientes: 0, templadas: 0, holgadas: 0 };
  const targets = claves.filter(c => {
    const normalized = normalizeClave(c);
    const st = state.get(c.toUpperCase());
    if (PINNED_KEYS.has(normalized)) { t.fijas++; return true; } // vigilancia dirigida → siempre
    if (!st) { nuevas++; return true; }        // nueva → siempre
    if (!st.isDue) return false;               // aún fresca para su nivel de cupos
    if (st.minDisp <= 3) t.criticas++;
    else if (st.minDisp <= 8) t.calientes++;
    else if (st.minDisp <= 15) t.templadas++;
    else t.holgadas++;
    return true;
  }).sort((a, b) => {
    // Las fijadas van primero para que ni un corte por presupuesto las deje fuera.
    const ap = PINNED_KEYS.has(normalizeClave(a));
    const bp = PINNED_KEYS.has(normalizeClave(b));
    if (ap !== bp) return ap ? -1 : 1;
    return byUrgency(a, b);
  });
  console.log(`   Modo priority inteligente → ${targets.length} claves | fijas:${t.fijas} criticas:${t.criticas} calientes:${t.calientes} templadas:${t.templadas} holgadas:${t.holgadas} nuevas:${nuevas} · omitidas ${claves.length - targets.length} frescas.`);
  return targets;
}

// ── Carril PONDERADO para quien PAGA (Pro/admin) ──────────────────────────────
// Objetivo: que en cada corrida las materias de los estudiantes Pro se revisen
// MUCHAS más veces que el resto, sin gastar ni una petición extra.
//
// Cómo, y por qué es seguro: NO abre pestañas, NO re-navega y NO cambia la
// concurrencia ni los gaps. Solo REORDENA y REPITE entradas en la cola que ya se
// iba a procesar → misma tasa de peticiones, misma sesión, CERO challenges de
// Cloudflare nuevos. (El incidente del 2026-07-24 lo causó una pestaña aparte que
// re-navegaba a la UASD; esta técnica estuvo activa horas sin un solo bloqueo.)
//
// Forma de la cola:
//   1) Barrido COMPLETO de las fijadas primero → si la corrida se corta o nos
//      frenan a los 2 minutos, lo de quien paga YA quedó al día.
//   2) Luego bloques de [N fijadas (rotando) + 1 regular] → las Pro se revisitan
//      varias veces por corrida mientras el catálogo general sigue avanzando.
// `PRO_LANE_RATIO` ajusta N (0 = desactivado, comportamiento histórico).
const PRO_LANE_RATIO = Math.max(0, Math.min(8, Number(process.env.PRO_LANE_RATIO ?? 2)));
let fastLaneOffset = 0;
function buildPriorityFastLane(targets) {
  if (PRO_LANE_RATIO === 0 || ETL_MODE !== 'priority' || targets.length === 0) return targets;

  const pinned  = targets.filter((c) => PINNED_KEYS.has(normalizeClave(c)));
  const regular = targets.filter((c) => !PINNED_KEYS.has(normalizeClave(c)));
  if (pinned.length === 0) return targets;

  // 1) Todas las fijadas de una, en su orden de urgencia.
  const queue = [...pinned];

  // 2) Intercalado ponderado con el resto del catálogo.
  let p = fastLaneOffset;
  for (const clave of regular) {
    for (let i = 0; i < PRO_LANE_RATIO; i++) {
      queue.push(pinned[p % pinned.length]);
      p++;
    }
    queue.push(clave);
  }
  // Rota el punto de partida entre pasadas para repartir parejo entre las Pro.
  fastLaneOffset = pinned.length ? (fastLaneOffset + 1) % pinned.length : 0;

  const revisitas = queue.length - regular.length;
  console.log(`   ⚡ Carril Pro/admin: ${pinned.length} clave(s) de pago al frente + ~${revisitas} revisita(s) intercaladas (ratio ${PRO_LANE_RATIO}:1). Misma tasa de peticiones.`);
  return queue;
}

// ── Circuit breaker IP-AWARE anti-castigo de Cloudflare ───────────────────────
// El bloqueo de CF es por REPUTACIÓN DE IP. Dos verdades que este breaker explota:
//   1) Martillar la MISMA IP bloqueada solo alarga el castigo → si seguimos en la
//      IP que ya nos bloqueó, ESPACIAMOS AGRESIVO (hasta 60 min) para que la IP
//      "descanse" y Cloudflare baje el flag.
//   2) Cuando la IP CAMBIA (el usuario prende el hotspot o reinicia el router),
//      esa IP nueva puede estar limpia → ATACAMOS DE UNA, sin esperar nada.
// Así el sistema se auto-recupera al instante apenas hay IP nueva, y mientras
// tanto no cava más hondo el hoyo. Devuelve null = "intenta ya".
// Racha de bloqueos detectada en el último chequeo del breaker. La usa el
// lanzador: con racha alta conviene arrancar con perfil LIMPIO, porque la
// `cf_clearance` guardada puede estar caducada/marcada y reusarla garantiza otro
// rechazo (el perfil persistente ayuda cuando la clearance es válida, no cuando
// ya nos rebotaron 10 veces).
let blockedStreak = 0;
const CLEAN_PROFILE_AFTER_STREAK = Math.max(1, Number(process.env.CLEAN_PROFILE_AFTER_STREAK || 4));

async function cloudflareCooldown(currentIp) {
  if (IGNORE_COOLDOWN) return null;
  try {
    const { data, error } = await supabase
      .from('etl_runs')
      .select('started_at, finished_at, status, public_ip')
      .order('started_at', { ascending: false })
      // Ventana amplia a propósito: cada espera escribe una fila `cooling`, así
      // que con una racha larga las filas `blocked` quedan lejos. Con 12 se
      // perdían de vista y el breaker atacaba creyendo que no había racha.
      .limit(40);
    if (error || !Array.isArray(data)) return null;

    let streak = 0, lastAttempt = null, lastBlockedIp = null;
    for (const r of data) {
      if (r.status === 'cooling') continue;          // espera, no intento
      if (r.status === 'running') continue;          // huérfana; no es señal
      if (r.status === 'blocked') {
        streak++;
        if (!lastAttempt) { lastAttempt = r.finished_at || r.started_at; lastBlockedIp = r.public_ip || null; }
        continue;
      }
      break;                                         // un intento no-bloqueado corta la racha
    }
    blockedStreak = streak;   // lo lee el lanzador para decidir el perfil
    if (!streak || !lastAttempt) return null;

    // IP NUEVA vs la que nos bloqueó → atacar YA (podría estar limpia).
    if (currentIp && lastBlockedIp && currentIp !== lastBlockedIp) {
      console.log(`   ↻ IP cambió (${lastBlockedIp} → ${currentIp}): intento inmediato con IP nueva.`);
      return null;
    }

    // Misma IP (o IP desconocida): escalera de reposo PROGRESIVA.
    //
    // Racha BAJA (1-2): esperas cortas. Un bloqueo suelto no es castigo duro; la
    // IP residencial suele seguir entregando parciales, así que no vale la pena
    // dejar a los estudiantes sin datos por un tropiezo.
    //
    // Racha ALTA (4+): descanso DE VERDAD. Evidencia del incidente 2026-07-24: con
    // el tope anterior de 15 min el sistema tocó la UASD cada ~15-20 min durante
    // >2 HORAS y jamás pasó — cada intento fallido es otro challenge que RENUEVA
    // el flag de Cloudflare. Insistir no acorta el castigo: lo alarga. Con la
    // racha alta conviene callarse 25-45 min y volver con la reputación limpia.
    // (Si el usuario cambia de IP —hotspot/router— el caso de arriba ataca ya.)
    const waitMin = [0, 3, 8, 15, 25, 35, 45][Math.min(streak, 6)];
    const elapsedMin = (Date.now() - Date.parse(lastAttempt)) / 60_000;
    // Recuperación tras un corte largo (WiFi caído, PC dormida): si el último
    // intento es MUY viejo, la racha es historia de antes del corte → no
    // arrastrarla. El umbral va holgado por encima del reposo máximo para no
    // anularlo (antes eran 20 min y se comía cualquier descanso largo).
    if (elapsedMin >= 90) return null;
    if (elapsedMin < waitMin) return { streak, waitMin, remainingMin: waitMin - elapsedMin, sameIp: !!lastBlockedIp };
    return null;
  } catch { return null; }   // sin telemetría no hay breaker: se intenta normal
}

// ── Ciclo de vida del navegador (renovable) ───────────────────────────────────
// Mutables a nivel de módulo para que hardRecover() pueda RELANZAR Chromium a
// mitad de corrida (browser crasheado/zombie) sin perder el progreso: lo ya
// upserteado quedó en Supabase y la cola sigue donde iba.
let browser = null, context = null, masterPage = null;
let proHunterPage = null;
let proHunterPageContext = null;
let proHunterStopRequested = false;
let proHunterPromise = null;
const proHunterLastNrcs = new Map();

// Proxy activo para esta corrida (null = salida directa por la IP de la PC).
// Se puede fijar a mano (SCRAPE_PROXY) o dejar que el cazador lo encuentre.
let activeProxy = (process.env.SCRAPE_PROXY || '').trim() || null;
const TRY_PROXY  = process.env.ETL_TRY_PROXY === 'true';

const CHROME_ARGS = [
  '--no-sandbox',
  '--disable-setuid-sandbox',
  '--disable-dev-shm-usage',
  '--disable-blink-features=AutomationControlled',
  '--disable-features=IsolateOrigins,site-per-process',
  '--lang=es-DO',
];
// Fingerprint coherente: NO forzamos User-Agent → se usa el real del Chromium
// headful (sin "HeadlessChrome" ni desajuste de versión que delate al bot).
// Locale/zona horaria de RD para parecer un usuario legítimo.
const CONTEXT_OPTS = {
  locale: 'es-DO',
  timezoneId: 'America/Santo_Domingo',
  viewport: { width: 1366, height: 768 },
};

// ── Cazador de PROXY GRATIS ───────────────────────────────────────────────────
// Descarga listas públicas de proxies gratis y prueba cada uno abriendo la UASD
// con un navegador real por ese proxy. Usa el PRIMERO que pase Cloudflare. Es
// gratis y no requiere intervención. OJO: la mayoría de los proxies gratis son
// de datacenter (Cloudflare suele bloquearlos), así que puede que ninguno pase;
// por eso está time-boxed y cae a salida directa si no encuentra ninguno.
// (2026-11-10, repo Runner) Pool de proxies en `proxies.mjs`: varias fuentes abiertas,
// prefiltro de red en paralelo (¿abre túnel hasta la UASD:443?) y prueba con navegador
// real de 3 en 3. Los que pasaron Cloudflare se recuerdan en PROXY_CACHE (archivo que
// el workflow conserva entre corridas) y se prueban PRIMERO la vez siguiente.
const PROXY_CACHE = String(process.env.PROXY_CACHE || '').trim();
function leerBuenos() {
  try { return PROXY_CACHE && fs.existsSync(PROXY_CACHE) ? JSON.parse(fs.readFileSync(PROXY_CACHE, 'utf8')).slice(0, 20) : []; } catch { return []; }
}
function guardarBueno(p) {
  if (!PROXY_CACHE) return;
  try { const v = [p, ...leerBuenos().filter((x) => x !== p)].slice(0, 20); fs.writeFileSync(PROXY_CACHE, JSON.stringify(v)); } catch { /* best-effort */ }
}
function olvidarBueno(p) {
  if (!PROXY_CACHE || !p) return;
  try { fs.writeFileSync(PROXY_CACHE, JSON.stringify(leerBuenos().filter((x) => x !== p))); } catch { /* best-effort */ }
}

// Prueba un proxy: abre UASD por él con navegador real; devuelve true si CF pasó.
async function testProxy(proxyStr, budgetMs = 22_000) {
  let b = null;
  try {
    b = await chromium.launch({ headless: HEADLESS, args: CHROME_ARGS, proxy: proxyArg(proxyStr) });
    const ctx = await b.newContext(CONTEXT_OPTS);
    await ctx.addInitScript(() => { Object.defineProperty(navigator, 'webdriver', { get: () => undefined }); });
    const pg = await ctx.newPage();
    await withTimeout(pg.goto(UASD_PAGE, { waitUntil: 'domcontentloaded', timeout: budgetMs }), budgetMs + 2000, 'timeout');
    let t = await pg.title().catch(() => '');
    const hasta = Date.now() + 20_000;
    while (looksBlocked(t) && !isHardBlock(t) && Date.now() < hasta) { await sleep(2500); t = await pg.title().catch(() => ''); }
    return !looksBlocked(t) && !isHardBlock(t);
  } catch { return false; }
  finally { try { if (b) await withTimeout(b.close(), 8000, 'close'); } catch {} }
}

// Busca un proxy que pase CF, con presupuesto total de tiempo. Devuelve el
// proxy ganador o null. Primero los que ya funcionaron antes; luego el pool.
async function huntWorkingProxy(maxMinutes = 6) {
  const deadline = Date.now() + maxMinutes * 60_000;
  for (const p of leerBuenos()) {
    if (Date.now() > deadline) break;
    if (await testProxy(p)) { console.log(`   ✅ Proxy recordado sigue pasando CF: ${p}`); guardarBueno(p); return p; }
    olvidarBueno(p);
  }
  console.log('   🔎 Buscando proxy público que pase Cloudflare…');
  const { listaDeProxies, prefiltrar } = await import('./proxies.mjs');
  const { lista, conteo } = await listaDeProxies();
  console.log(`   ${lista.length} candidatos ${JSON.stringify(conteo)}. Prefiltro de red…`);
  const vivos = await prefiltrar(lista, { quiero: 36, hastaMs: Math.min(90_000, Math.max(10_000, deadline - Date.now() - 60_000)) });
  console.log(`   ${vivos.length} abren túnel hasta la UASD. Probando con navegador (de 3 en 3)…`);
  let probados = 0;
  for (let i = 0; i < vivos.length && Date.now() < deadline; i += 3) {
    const tanda = vivos.slice(i, i + 3).map((v) => v.url);
    const res = await Promise.all(tanda.map((p) => testProxy(p)));
    probados += tanda.length;
    const k = res.findIndex(Boolean);
    if (k >= 0) { console.log(`   ✅ Proxy que pasa CF: ${tanda[k]} (tras ${probados} pruebas).`); guardarBueno(tanda[k]); return tanda[k]; }
  }
  console.log(`   ✗ Ningún proxy pasó CF (${probados} probados con navegador).`);
  return null;
}

// Objeto proxy para Playwright a partir de "http://user:pass@host:port" o "host:port".
function proxyArg(p) {
  if (!p) return undefined;
  try {
    const u = new URL(/^\w+:\/\//.test(p) ? p : `http://${p}`);
    const server = `${u.protocol}//${u.host}`;
    const out = { server };
    if (u.username) out.username = decodeURIComponent(u.username);
    if (u.password) out.password = decodeURIComponent(u.password);
    return out;
  } catch { return { server: p.startsWith('http') || p.startsWith('socks') ? p : `http://${p}` }; }
}

async function launchBrowserAndPage() {
  const px = proxyArg(activeProxy);
  console.log(`   Iniciando Chromium (headless=${HEADLESS}${PROFILE_DIR ? ` · perfil=${PROFILE_DIR}` : ''}${px ? ` · proxy=${px.server}` : ''})…`);
  // Con proxy usamos perfil EFÍMERO (el persistente puede arrastrar cookies de
  // otra IP y confundir el challenge). Sin proxy, perfil persistente si hay.
  // Racha alta = la clearance guardada ya no sirve (nos rebotaron una y otra vez).
  // Arrancar limpio da una huella nueva y una oportunidad real de resolver el
  // challenge, en vez de repetir el mismo rechazo con la cookie quemada.
  const perfilSucio = blockedStreak >= CLEAN_PROFILE_AFTER_STREAK;
  if (perfilSucio && PROFILE_DIR) {
    console.warn(`   ⚠ Racha de ${blockedStreak} bloqueos: ignorando el perfil persistente y arrancando LIMPIO (clearance quemada).`);
  }
  if (PROFILE_DIR && !px && !perfilSucio) {
    // Perfil persistente: cookies (cf_clearance) e historial sobreviven entre
    // corridas → menos challenges = mejor reputación ante Cloudflare. Si el
    // perfil quedó bloqueado por un Chromium zombie, caemos a perfil efímero
    // para NO perder el ciclo (el watchdog local mata al zombie después).
    try {
      context = await chromium.launchPersistentContext(PROFILE_DIR, {
        headless: HEADLESS, args: CHROME_ARGS, ...CONTEXT_OPTS,
      });
      browser = context.browser();
    } catch (e) {
      console.warn(`   ⚠ Perfil persistente no disponible (${String(e.message).slice(0, 120)}); usando perfil efímero este ciclo.`);
      context = null;
    }
  }
  if (!context) {
    browser = await chromium.launch({ headless: HEADLESS, args: CHROME_ARGS, ...(px ? { proxy: px } : {}) });
    context = await browser.newContext(CONTEXT_OPTS);
  }
  // Oculta navigator.webdriver (señal típica de automatización que CF detecta).
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  });
  console.log('   Cargando página UASD y resolviendo Cloudflare…');
  masterPage = await openPage(context);
}

async function closeBrowserSafe() {
  await closeProHunterPageSafe();
  // close() sobre un browser zombie puede colgarse también → con límite.
  try { if (context) await withTimeout(context.close(), 15_000, 'close colgado'); } catch { /* ya está muerto */ }
  try { if (browser) await withTimeout(browser.close(), 10_000, 'close colgado'); } catch { /* idem */ }
  browser = null; context = null; masterPage = null;
}

// ── Carril PRO aislado (5 s) + autodoctor propio ─────────────────────────────
// Usa una pestaña secundaria dentro de la sesión ya validada por Cloudflare.
// Nunca recarga/cierra masterPage y jamás cambia la concurrencia/cola del ETL
// general. Si esta pestaña falla, solo ella se destruye y se vuelve a crear.
async function closeProHunterPageSafe() {
  const page = proHunterPage;
  proHunterPage = null;
  proHunterPageContext = null;
  if (!page) return;
  try { await withTimeout(page.close(), 5_000, 'close PRO colgado'); } catch { /* aislado */ }
}

async function ensureProHunterPage() {
  if (!context) throw new Error('PRO_PAGE::contexto no disponible');
  if (
    proHunterPage &&
    proHunterPageContext === context &&
    !proHunterPage.isClosed()
  ) return proHunterPage;

  await closeProHunterPageSafe();
  const page = await context.newPage();
  try {
    await page.goto(UASD_PAGE, { waitUntil: 'domcontentloaded', timeout: 45_000 });
    await sleep(1_500);
    let title = await page.title().catch(() => '');
    if (looksBlocked(title)) {
      const recovered = await recoverPage(page);
      title = await page.title().catch(() => title);
      if (!recovered || looksBlocked(title)) {
        throw new Error(`PRO_CF::${title || 'challenge'}`);
      }
    }
    proHunterPage = page;
    proHunterPageContext = context;
    return page;
  } catch (e) {
    try { await page.close(); } catch { /* noop */ }
    throw e;
  }
}

function sameStringSet(a, b) {
  if (!a || !b || a.size !== b.size) return false;
  for (const value of a) if (!b.has(value)) return false;
  return true;
}

async function persistProSnapshot(clave, rawRows) {
  const mapped = (Array.isArray(rawRows) ? rawRows : [])
    .map(mapRow)
    .filter((row) => row.periodo && row.nrc);
  const rows = [...new Map(mapped.map((row) => [`${row.periodo}|${row.nrc}`, row])).values()];

  // Escritura inmediata para que scraped_at y los avisos reflejen esta consulta
  // real. Solo filas de UNA materia; no ejecuta refresh global ni funciones Vercel.
  for (let i = 0; i < rows.length; i += 500) {
    const { error } = await withTimeout(
        supabase
          .from('cupos_oficiales')
          .upsert(rows.slice(i, i + 500), { onConflict: 'periodo,nrc', ignoreDuplicates: false }),
        15_000,
        'PRO_DB_TIMEOUT'
      )
      .catch((e) => { throw new Error(`PRO_DB::${e?.message || e}`); });
    if (error) throw new Error(`PRO_DB::${error.message}`);
  }

  // La reconciliación requiere SELECT; solo se hace en la primera lectura o si
  // cambió la lista de NRC. Si todo sigue igual, evita egress innecesario.
  const key = normalizeClave(clave);
  const currentNrcs = new Set(rows.map((row) => row.nrc));
  const previousNrcs = proHunterLastNrcs.get(key);
  let cerradas = 0;
  if (!sameStringSet(previousNrcs, currentNrcs)) {
    cerradas = await withTimeout(
        cerrarSeccionesDesaparecidas(new Map([[key, currentNrcs]])),
        20_000,
        'PRO_DB_RECONCILE_TIMEOUT'
      )
      .catch((e) => { throw new Error(`PRO_DB::${e?.message || e}`); });
    proHunterLastNrcs.set(key, currentNrcs);
  }
  return { rows: rows.length, cerradas };
}

async function scrapeProKey(clave) {
  const page = await ensureProHunterPage();
  const rows = await pageXHR(page, 'getData', { campus: '000', clave });
  return persistProSnapshot(clave, rows);
}

async function sleepProHunter(ms) {
  const until = Date.now() + Math.max(0, ms);
  while (!proHunterStopRequested && Date.now() < until) {
    await sleep(Math.min(1_000, until - Date.now()));
  }
}

async function runProHunter() {
  let cursor = 0;
  let failureStreak = 0;
  let checks = 0;
  let lastStatusLog = 0;

  console.log(`   ⚡ [PRO] carril aislado activo: 1 consulta cada ${PRO_HUNTER_INTERVAL_MS / 1_000}s.`);
  while (!proHunterStopRequested && minutesLeft() > 1) {
    const tickStarted = Date.now();
    const keys = await refreshProHunterKeys();
    if (!keys.length) {
      if (Date.now() - lastStatusLog > 60_000) {
        console.log('   ⚡ [PRO] sin objetivos activos; esperando sin consumir UASD.');
        lastStatusLog = Date.now();
      }
      await sleepProHunter(PRO_HUNTER_INTERVAL_MS);
      continue;
    }

    if (cursor >= keys.length) cursor = 0;
    const clave = keys[cursor++];
    try {
      const result = await scrapeProKey(clave);
      failureStreak = 0;
      checks++;
      if (Date.now() - lastStatusLog > 60_000) {
        console.log(
          `   ⚡ [PRO] vivo · ${checks} consulta(s) · ${keys.length} clave(s) activas · ` +
          `última=${clave} filas=${result.rows}${result.cerradas ? ` cerradas=${result.cerradas}` : ''}`
        );
        lastStatusLog = Date.now();
      }
    } catch (e) {
      failureStreak++;
      const message = String(e?.message || e);
      const dbOnly = message.startsWith('PRO_DB::');
      const protectedFailure = /PRO_CF::|IP_BLOQUEADA|Cloudflare|HTTP (403|429|500|502|503)|isHTML=true|rate-limit|penaliz/i.test(message);
      if (!dbOnly) await closeProHunterPageSafe();

      // Autodoctor SOLO PRO. Los frenos de UASD reciben descanso exponencial;
      // un fallo de Supabase conserva la sesión y vuelve a intentar después.
      const base = protectedFailure ? 15_000 : PRO_HUNTER_INTERVAL_MS;
      const backoff = Math.min(
        PRO_HUNTER_MAX_BACKOFF_MS,
        base * Math.pow(2, Math.min(failureStreak - 1, 5))
      );
      console.warn(
        `   ⚠ [PRO] autodoctor ${failureStreak}: ${message.slice(0, 180)} · ` +
        `pausa ${(backoff / 1_000).toFixed(0)}s (ETL general intacto).`
      );
      await sleepProHunter(backoff);
      continue;
    }

    // Cadencia entre INICIOS. Si la respuesta tomó más de 5 s, no acumula ticks:
    // continúa inmediatamente, siempre con una sola solicitud PRO a la vez.
    await sleepProHunter(Math.max(0, PRO_HUNTER_INTERVAL_MS - (Date.now() - tickStarted)));
  }
  await closeProHunterPageSafe();
  console.log('   ⚡ [PRO] carril detenido de forma ordenada.');
}

function startProHunter() {
  if (!PRO_HUNTER_ENABLED || proHunterPromise) return;
  proHunterStopRequested = false;
  proHunterPromise = runProHunter()
    .catch((e) => console.warn('   ⚠ [PRO] salida protegida:', e?.message || e))
    .finally(() => { proHunterPromise = null; });
}

async function stopProHunter() {
  proHunterStopRequested = true;
  if (proHunterPromise) {
    const running = proHunterPromise;
    const stopped = await withTimeout(running, 12_000, 'stop PRO agotado')
      .then(() => true)
      .catch(() => false);
    if (!stopped) {
      // Si estaba dentro de un XHR colgado, cerrar SOLO su pestaña desbloquea la
      // promesa. Esperamos una última ventana corta antes de cerrar el contexto.
      await closeProHunterPageSafe();
      await withTimeout(running, 5_000, 'stop PRO forzado').catch(() => {});
    }
  }
  await closeProHunterPageSafe();
}

async function hardRecover(reason) {
  console.warn(`   ♻ Relanzando Chromium (${reason})…`);
  await closeBrowserSafe();
  await sleep(3000 + jitter());
  await launchBrowserAndPage();
}

// ¿El error indica browser/página muertos o colgados? → amerita relanzar.
function isDeadBrowserError(msg) {
  return /target closed|browser has been closed|browser closed|page.*closed|crashed|disconnected|LOTE_COLGADO/i.test(String(msg || ''));
}

// ── Main ───────────────────────────────────────────────────────────────────────
async function main() {
  const t0 = Date.now();
  console.log('══════════════════════════════════════════');
  console.log('   ETL cupos_oficiales (Playwright XHR)');
  const modoInfo = ETL_MODE === 'priority' ? ` (graduado por cupos · speed=${PRIORITY_SPEED})`
    : ETL_MODE === 'stale' ? ` (>${STALE_HOURS}h)` : '';
  console.log(`   modo=${ETL_MODE}${modoInfo} | fetch concurrentes=${FETCH_CONC} | lote=${BATCH_SIZE}`);
  if (DEADLINE_MIN) console.log(`   presupuesto de tiempo: ${DEADLINE_MIN} min (corte ordenado, lo urgente va primero)`);
  console.log(`   ${new Date().toISOString()}`);
  console.log('══════════════════════════════════════════\n');

  const myIp = await getPublicIP();
  console.log(`   IP pública de salida: ${myIp || 'desconocida'}`);

  // ── Cazador de proxy gratis (si está activado y no hay proxy fijo) ─────────
  // Si encuentra uno que pase Cloudflare, la salida ya NO es la IP bloqueada de
  // la casa → nos saltamos el enfriamiento y atacamos con el proxy.
  if (TRY_PROXY && !activeProxy) {
    // (2026-11-10, Runner) Primero la salida DIRECTA: medido en GitHub Actions, la IP del
    // runner pasa Cloudflare y es más rápida y fiable que cualquier proxy gratis. El proxy
    // solo entra si la directa queda bloqueada.
    if (await testProxy(null, 25_000)) {
      console.log('   ✓ La salida directa pasa Cloudflare: sin proxy en este ciclo.');
    } else {
      const found = await huntWorkingProxy(Number(process.env.PROXY_HUNT_MIN || 6));
      if (found) { activeProxy = found; console.log(`   → Usando proxy gratis: ${activeProxy}`); }
    }
  }

  // ── Enfriamiento anti-bloqueo IP-AWARE (ANTES de tocar la UASD) ───────────
  // Con proxy activo, la IP de salida es la del proxy: el enfriamiento de la IP
  // de la casa no aplica.
  const cd = activeProxy ? null : await cloudflareCooldown(myIp);
  if (cd) {
    console.warn(`::notice::Cloudflare lleva ${cd.streak} bloqueo(s) seguido(s) en ESTA MISMA IP (${myIp || '?'}); ` +
      `dejándola descansar (${cd.waitMin} min entre intentos, faltan ~${cd.remainingMin.toFixed(0)} min). ` +
      `Apenas cambie la IP (hotspot/router) el sistema ataca de una. Este ciclo NO toca la UASD.`);
    await hbStart();
    await hbEnd('cooling', { detail: `racha de ${cd.streak} bloqueos CF en IP ${myIp || '?'}; próximo intento en ~${cd.remainingMin.toFixed(0)} min (o inmediato si cambia la IP)` });
    return;
  }

  await hbStart();

  // ── Browser ───────────────────────────────────────────────────────────────
  console.log('0. Navegador…');
  await launchBrowserAndPage();
  console.log('   ✓ Página lista. Título:', await masterPage.title(), '\n');
  startProHunter();

  // ── 1. getCampus ─────────────────────────────────────────────────────────
  console.log('1. getCampus…');
  const campusList = await pageXHR(masterPage, 'getCampus');
  console.log(`   ${campusList.length} campus.`);

  const campusRows = campusList
    .filter(c => c.codigo && c.codigo !== '000')
    .map(c => ({ codigo: String(c.codigo).trim(), descripcion: String(c.descripcion ?? '').trim() }));

  const { error: ce } = await supabase.from('campus_catalogo').upsert(campusRows, { onConflict: 'codigo', ignoreDuplicates: false });
  if (ce) console.warn('   ⚠ campus_catalogo:', ce.message);
  else console.log(`   ✓ ${campusRows.length} campus guardados.\n`);

  // ── 2. Universo de claves ─────────────────────────────────────────────────
  console.log('2. Universo de claves…');
  const claveSet = new Set();

  try {
    const a = await pageXHR(masterPage, 'getAsignaturas', { campus: '000' });
    if (Array.isArray(a) && a.length) { a.forEach(k => claveSet.add(String(k).trim())); }
    console.log(`   getAsignaturas(000) → ${claveSet.size} claves.`);
  } catch (e) { console.warn('   ⚠ getAsignaturas(000):', e.message); }

  if (claveSet.size < 100) {
    console.log('   Completando por campus…');
    for (const cod of campusRows.map(c => c.codigo)) {
      await sleep(jitter());
      try {
        const a = await pageXHR(masterPage, 'getAsignaturas', { campus: cod });
        if (Array.isArray(a)) a.forEach(k => claveSet.add(String(k).trim()));
      } catch (e) { console.warn(`   ⚠ getAsignaturas(${cod}):`, e.message); }
    }
  }

  const universo = [...claveSet].filter(Boolean).sort();
  // Reparto en turnos paralelos (ETL_SHARDS): cada turno corre en su propia máquina
  // de GitHub, con su propia IP, y se queda con una parte fija del catálogo (hash de
  // la clave). Misma cortesía por IP; el catálogo entero se refresca N veces más rápido.
  const claves = SHARD_TOTAL > 1 ? universo.filter((c) => shardDe(c) === SHARD_INDEX) : universo;
  // Clave normalizada → clave tal como la lista la UASD (para el carril de demanda).
  const clavePorNorm = new Map(claves.map((c) => [normalizeClave(c), c]));
  console.log(`   Total universo: ${universo.length} claves${SHARD_TOTAL > 1 ? ` · turno ${SHARD_INDEX + 1}/${SHARD_TOTAL}: ${claves.length}` : ''}.`);
  if (!claves.length) {
    await hbEnd('error', { detail: 'getAsignaturas devolvió 0 claves' });
    await stopProHunter();
    await closeBrowserSafe();
    console.error('❌ Sin claves.');
    process.exit(1);
  }

  // ── 2b. Filtrado por modo (eficiencia) ────────────────────────────────────
  // state es MUTABLE: el bucle de re-priorización (priority) lo re-lee en cada
  // pasada para saber qué volvió a vencer. runList lo referencia por clausura.
  let state = await getClaveState();
  // En priority, suma las claves vigiladas del equipo (best-effort) para que su
  // proyección entre siempre en el ciclo. En full/stale/delta no hace falta.
  if (ETL_MODE === 'priority') await loadDynamicPinnedKeys();

  // ── 3. getData — motor educado con backoff adaptativo y reintentos ──────────
  const PROFILE_MAIN   = {
    // El general conserva EXACTAMENTE su concurrencia histórica. El recurso
    // adicional vive en runProHunter(), nunca se presta a la cola Free.
    conc: FETCH_CONC,
    gapMin: GAP_MIN,
    gapMax: GAP_MAX,
    tries: 4,
  };
  // Perfil suave para reintentos: más lento aún, para vencer el rate-limit.
  const PROFILE_GENTLE = { conc: 1, gapMin: GAP_MIN * 2, gapMax: GAP_MAX * 2, tries: 5 };

  let totalRows = 0, okCount = 0, seccionesNuevas = 0, processedTotal = 0, stoppedByDeadline = false;
  let totalCerradas = 0, errors = 0, lastTargets = 0;

  // Procesa una lista de claves; devuelve las que quedaron SIN datos (para reintentar).
  async function runList(list, opts, label) {
    const failed = [];
    let processed = 0;
    for (let b = 0; b < list.length; b += BATCH_SIZE) {
      // PRESUPUESTO: si se agotó el tiempo, corta ORDENADO. Lo urgente ya se
      // procesó (la cola va por atraso); el resto entra en el próximo ciclo.
      // Mil veces mejor que dejar que el timeout del job mate el run a mitad
      // de un upsert y tapone la cola de concurrencia por horas.
      if (minutesLeft() <= 1) {
        stoppedByDeadline = true;
        console.warn(`\n   ⏱ Presupuesto de ${DEADLINE_MIN} min agotado en [${label}]: corte ordenado con ${processed}/${list.length} procesadas.`);
        break;
      }
      const batch = list.slice(b, b + BATCH_SIZE);
      let res;
      try {
        // withTimeout: un page.evaluate sobre un browser zombie NUNCA resuelve;
        // sin este límite, un solo cuelgue se comía el timeout completo del job.
        res = await withTimeout(fetchBatch(masterPage, batch, opts), BATCH_HARD_TIMEOUT, 'LOTE_COLGADO');
      } catch (e) {
        if (isDeadBrowserError(e.message)) {
          await hardRecover(e.message);
        } else {
          console.warn(`\n   ⚠ lote falló (${e.message}); re-resolviendo Cloudflare…`);
          const ok = await recoverPage(masterPage).catch(() => false);
          if (!ok) await hardRecover('recoverPage agotado');
        }
        res = await withTimeout(fetchBatch(masterPage, batch, opts), BATCH_HARD_TIMEOUT, 'LOTE_COLGADO')
          .catch(() => batch.map(c => ({ clave: c, ok: false, rateLimited: true })));
      }

      const toUpsert = [];
      // clave (UPPER) → Set<nrc> que la UASD lista AHORA. Solo claves scrapeadas OK.
      // Con secciones → base para cerrar las que desaparecieron; con lista VACÍA →
      // Set vacío (materia 100% llena: hay que cerrar TODAS sus secciones).
      const scrapedByClave = new Map();
      let limitedInBatch = 0, blockedInBatch = 0;
      for (const r of res) {
        if (r.ok) {
          okCount++;
          const claveUp = (r.clave || '').toUpperCase();
          if (Array.isArray(r.rows) && r.rows.length) {
            const mapped = r.rows.map(mapRow).filter(x => x.periodo && x.nrc);
            const unicos = Array.from(new Map(mapped.map(it => [it.nrc, it])).values());
            toUpsert.push(...unicos);
            if (unicos.length && claveUp) scrapedByClave.set(claveUp, new Set(unicos.map(u => u.nrc)));
            // Detección de SECCIONES NUEVAS: si trae más secciones que las que había.
            const prevN = state.get(claveUp)?.nSecc ?? unicos.length;
            if (unicos.length > prevN) {
              const delta = unicos.length - prevN;
              seccionesNuevas += delta;
              console.log(`\n   ✨ ${r.clave}: +${delta} sección(es) nueva(s) (${prevN}→${unicos.length}).`);
            }
          } else if (claveUp && Array.isArray(r.rows) && r.rows.length === 0) {
            // OK y la UASD devolvió lista VACÍA → la materia ya no lista NINGUNA
            // sección (todas llenas). Set vacío → cerrarSeccionesDesaparecidas cierra
            // TODAS las secciones que la BD aún muestra con cupo. Mata el cupo fantasma
            // del caso "materia 100% llena", que antes quedaba SIN reconciliar (la fila
            // se congelaba con su último disponibles positivo para siempre).
            scrapedByClave.set(claveUp, new Set());
          }
        } else {
          failed.push(r.clave);
          if (r.rateLimited) limitedInBatch++;
          if (r.blocked) blockedInBatch++;
        }
      }
      if (toUpsert.length) { await upsertRows(toUpsert); totalRows += toUpsert.length; }
      // Cierra las secciones que la UASD dejó de listar (llenas) → mata los cupos
      // fantasma que reportaban los estudiantes. Solo toca claves scrapeadas OK.
      if (scrapedByClave.size) {
        const cerradas = await cerrarSeccionesDesaparecidas(scrapedByClave);
        if (cerradas) { totalCerradas += cerradas; console.log(`\n   🔒 ${cerradas} sección(es) cerrada(s) (ya no listadas por la UASD).`); }
      }
      processed += batch.length;
      processedTotal += batch.length;
      process.stdout.write(`\r   [${label}] ${processed}/${list.length} | filas:${totalRows} | ok:${okCount} | pendientes:${failed.length}   `);
      // Latido por lote: el watchdog externo distingue "trabajando" de "colgado".
      await hbBeat({ procesadas: processedTotal, filas: totalRows, secciones_nuevas: seccionesNuevas });

      // ADAPTATIVO: si CF re-desafió, re-resolver; si nos frenaron, enfriar.
      if (blockedInBatch > batch.length * 0.3) {
        console.warn(`\n   ⚠ Cloudflare re-desafió; re-resolviendo…`);
        await recoverPage(masterPage);
      } else if (limitedInBatch > batch.length * 0.3) {
        const cool = 8000 + Math.random() * 4000;
        console.warn(`\n   ⏸ La UASD nos frenó (${limitedInBatch}/${batch.length}); enfriando ${(cool / 1000).toFixed(0)}s…`);
        await sleep(cool);
      }

      // Carril de demanda: lo que los estudiantes acaban de abrir entra justo después de este lote.
      if (await revisarDemanda(clavePorNorm)) {
        const pedidas = tomarDemanda();
        list.splice(b + BATCH_SIZE, 0, ...pedidas);
        console.log(`\n   👀 Demanda: ${pedidas.length} materia(s) que están consultando entran al frente (${pedidas.join(', ')}).`);
      }
    }
    return failed;
  }

  // ── 3b. Reconciliación por UNIVERSO — cierra claves que la UASD ya no lista ──
  // getAsignaturas (paso 2) es la lista MAESTRA de lo que EXISTE hoy. Una clave que
  // la BD aún muestra con cupo pero que YA NO aparece en el universo = la UASD la
  // retiró (se llenó del todo o se cerró) → cupo fantasma que se congela para
  // SIEMPRE, porque esa clave nunca vuelve a entrar a un lote de scrape. La cerramos
  // (disponibles=0), SIN borrar la fila (SEO vivo, regla 2) y SIN una sola petición
  // extra a la UASD (regla 6): reusa el universo ya descargado. GATE DE SEGURIDAD:
  // solo si el universo vino sano (≥80% de las claves en BD); si getAsignaturas
  // viniera truncado por un glitch, se omite en vez de vaciar el catálogo.
  async function reconcileUniverse() {
    try {
      const universeUpper = new Set(claves.map(c => c.toUpperCase()));
      if (claves.length >= Math.max(500, state.size * 0.8)) {
        const ghosts = new Map();
        for (const claveUp of state.keys()) if (!universeUpper.has(claveUp)) ghosts.set(claveUp, new Set());
        if (ghosts.size) {
          const cerradasGhost = await cerrarSeccionesDesaparecidas(ghosts);
          if (cerradasGhost) {
            totalCerradas += cerradasGhost;
            console.log(`   🔒 ${cerradasGhost} sección(es) cerrada(s) de ${ghosts.size} clave(s) fuera del universo (la UASD ya no las lista).`);
          }
        }
      } else {
        console.warn(`   ⚠ Universo pequeño (${claves.length} claves vs ${state.size} en BD); se omite la reconciliación por universo (seguridad).`);
      }
    } catch (e) { console.warn('   ⚠ reconciliación por universo:', e?.message || e); }
  }

  // ── Bucle de RE-PRIORIZACIÓN CONTINUA (pico de selección) ───────────────────
  // En priority, mientras quede presupuesto, RE-EVALÚA la urgencia y vuelve a pasar
  // sobre lo más crítico (su presupuesto corto lo deja "vencido" de nuevo en
  // minutos), reusando el MISMO navegador caliente → una sola resolución de
  // Cloudflare por corrida (mejor reputación de IP, no peor). MISMA tasa de
  // peticiones que hoy (conc/gaps intactos): solo reparte el mismo caudal hacia lo
  // crítico. full/stale/delta = UNA sola pasada (idéntico al comportamiento previo).
  const LOOP_MARGIN_MIN = 3;   // aire para cerrar navegador + refrescar búsqueda al final
  // El bucle continuo SOLO aplica a priority CON presupuesto finito (el workflow
  // pone ETL_DEADLINE_MIN=50). Sin deadline (corrida local, DEADLINE_MIN=0)
  // minutesLeft()=Infinity → se hace UNA sola pasada, nunca un bucle infinito.
  const LOOP_ENABLED = ETL_MODE === 'priority' && DEADLINE_MIN > 0;
  let pass = 0;
  while (true) {
    pass++;
    if (pass > 1) {
      // Re-lee el estado (1 RPC agregada, barata) para saber qué volvió a vencer:
      // lo recién scrapeado ya no está due, y lo que envejeció sube al frente.
      state = await getClaveState();
      if (ETL_MODE === 'priority') await loadDynamicPinnedKeys();
    }
    // Lo que los estudiantes están consultando va primero en la pasada.
    await revisarDemanda(clavePorNorm, true);
    const pedidasPase = tomarDemanda();
    const selectedTargets = pedidasPase.length
      ? [...new Set([...pedidasPase, ...selectTargets(claves, state)])]
      : selectTargets(claves, state);
    if (pedidasPase.length) console.log(`   👀 Demanda: ${pedidasPase.length} materia(s) consultadas al frente de la pasada.`);
    lastTargets = selectedTargets.length;
    if (!selectedTargets.length) {
      // Nada vencido ahora mismo: en priority espera corta y re-evalúa (sin spin,
      // manteniendo el navegador caliente); en los demás modos, no hay nada → salir.
      if (LOOP_ENABLED && minutesLeft() > LOOP_MARGIN_MIN) {
        // Espera hasta que la PRÓXIMA clave de este turno vence (45 s – 10 min), en vez de
        // releer el estado completo cada 45 s: lo mismo de fresco, mucha menos salida de Supabase.
        let proximaMs = 600_000;
        for (const c of claves) {
          const st = state.get(c.toUpperCase());
          if (!st) continue;
          const faltaMs = (refreshBudgetHours(st.minDisp) - st.ageH) * 3_600_000;
          if (faltaMs < proximaMs) proximaMs = faltaMs;
        }
        const esperaMs = Math.max(45_000, Math.min(600_000, Math.round(proximaMs)), 0);
        await hbBeat({ detail: `pasada ${pass}: nada vencido; espera ${Math.round(esperaMs / 1000)} s` });
        // El reposo se interrumpe si un estudiante consulta una materia de este turno.
        const despiertaEn = Date.now() + Math.min(esperaMs, Math.max(45_000, (minutesLeft() - LOOP_MARGIN_MIN) * 60_000));
        while (Date.now() < despiertaEn) {
          await sleep(Math.min(DEMANDA_CADA_MS, Math.max(1_000, despiertaEn - Date.now())));
          if (await revisarDemanda(clavePorNorm)) { console.log('   👀 Demanda durante el reposo: se despierta.'); break; }
        }
        continue;
      }
      break;
    }
    const targets = buildPriorityFastLane(selectedTargets);
    await hbBeat({ targets: selectedTargets.length, scheduled: targets.length });
    console.log(`3. getData · pasada ${pass} (${targets.length} consultas · ${selectedTargets.length} claves elegibles · conc=${PROFILE_MAIN.conc} · gap=${GAP_MIN}-${GAP_MAX}ms · lote=${BATCH_SIZE})…`);

    // Pasada + hasta 3 rondas de reintento cada vez más suaves.
    let pending = await runList(targets, PROFILE_MAIN, `p${pass}`);
    for (let round = 1; round <= 3 && pending.length && !stoppedByDeadline && minutesLeft() > 8; round++) {
      console.log(`\n   ↻ Ronda ${round}: reintentando ${pending.length} claves más despacio…`);
      await sleep(6000);
      const ok = await recoverPage(masterPage).catch(() => false);
      if (!ok) await hardRecover('recoverPage agotado pre-reintento');
      pending = await runList(pending, PROFILE_GENTLE, `p${pass}r${round}`);
    }
    errors = new Set(pending.map(normalizeClave)).size;
    console.log(`\n   ✓ getData pasada ${pass} ${stoppedByDeadline ? 'CORTADA por presupuesto' : 'completa'}. Sin datos: ${errors}`);

    // Cierra fantasmas fuera del universo cada pasada (barato; tras limpiar, no-op).
    await reconcileUniverse();

    // full/stale/delta (o priority sin deadline): una sola pasada. priority con
    // presupuesto: repetir mientras quede tiempo y no nos hayan cortado.
    if (!LOOP_ENABLED) break;
    if (stoppedByDeadline || minutesLeft() <= LOOP_MARGIN_MIN) break;
    await hbBeat({ procesadas: processedTotal, filas: totalRows, secciones_nuevas: seccionesNuevas });
  }

  await stopProHunter();
  await closeBrowserSafe();

  // ── 4. Refresh búsqueda ────────────────────────────────────────────────────
  // Con turnos paralelos, solo el primero refresca la búsqueda (es global).
  if (SHARD_INDEX === 0) {
    console.log('4. refresh_asignaturas_busqueda…');
    const { error: re } = await supabase.rpc('refresh_asignaturas_busqueda');
    if (re) console.warn('   ⚠', re.message);
    else console.log('   ✓ OK.');
  }

  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\n══════════════════════════════════════════`);
  console.log(`   Pasadas:${pass} | Últimas claves:${lastTargets} | Procesadas(acum):${processedTotal} | Filas:${totalRows} | Secciones nuevas:${seccionesNuevas} | Cerradas(llenas):${totalCerradas} | Sin datos:${errors} | ${elapsed}s`);
  console.log(`══════════════════════════════════════════\n`);

  const tooManyMissing = lastTargets > 0 && errors > lastTargets * 0.3;
  const status = stoppedByDeadline ? 'partial' : (tooManyMissing ? 'degraded' : 'ok');
  await hbEnd(status, {
    targets: lastTargets, procesadas: processedTotal, filas: totalRows,
    secciones_nuevas: seccionesNuevas, sin_datos: errors,
    detail: stoppedByDeadline
      ? `corte ordenado a los ${DEADLINE_MIN} min; lo pendiente sigue en cola por urgencia`
      : (ETL_MODE === 'priority' ? `${pass} pasada(s) de re-priorización continua` : null),
  });

  // Falla el job SOLO si quedó mucho sin traer (problema real), no por rezagados
  // ni por corte de presupuesto (eso es comportamiento diseñado: exit 0).
  if (tooManyMissing && !stoppedByDeadline) {
    console.error(`❌ Demasiadas claves sin datos (${errors}/${lastTargets}); revisar rate-limit/CF.`);
    process.exit(1);
  }
}

main().catch(async (e) => {
  const msg = String((e && e.message) || e);
  await stopProHunter();
  await closeBrowserSafe();   // sin Chromium huérfano comiéndose la RAM del nodo
  // Bloqueo/freno temporal de Cloudflare NO es un bug: es condición transitoria de
  // la UASD. Salimos en VERDE (exit 0) con aviso; el próximo ciclo reintenta solo.
  if (/IP_BLOQUEADA|No se pudo pasar Cloudflare|rate-limit|penalizaci|access denied/i.test(msg)) {
    console.warn(`\n::warning::La UASD bloqueó/frenó temporalmente la IP. No se scrapeó este ciclo; se reintenta en el próximo. (${msg.slice(0, 120)})`);
    await hbEnd('blocked', { detail: msg.slice(0, 500) });
    process.exit(0);
  }
  console.error('❌', e);
  await hbEnd('error', { detail: msg.slice(0, 500) });
  process.exit(1);
});
