// ─────────────────────────────────────────────────────────────────────────────
// diagnostico.mjs — ¿Llega el runner de GitHub a la Programación de la UASD?
// ─────────────────────────────────────────────────────────────────────────────
// No necesita secretos ni escribe en ninguna base: abre la página de la UASD con
// un Chromium real (salida directa y luego por proxies públicos), espera a que
// Cloudflare decida y, si deja pasar, pide la lista de recintos (una sola
// llamada, la misma que hace el ETL al arrancar). Imprime solo cifras y estados.
// ─────────────────────────────────────────────────────────────────────────────

import { chromium } from 'playwright';
import { listaDeProxies, prefiltrar } from './proxies.mjs';

const PAGINA = 'https://app.uasd.edu.do/ProgramacionPorAsignatura/Default.aspx';
const HEADLESS = process.env.HEADLESS !== 'false';
const ARGS = ['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled', '--lang=es-DO'];
const CTX = { locale: 'es-DO', timezoneId: 'America/Santo_Domingo', viewport: { width: 1366, height: 768 } };
const bloqueado = (t) => { const s = (t || '').toLowerCase(); return !s || /just a moment|un momento|attention|cloudflare|access denied|acceso denegado/.test(s); };
const dormir = (ms) => new Promise((r) => setTimeout(r, ms));

async function probar(proxy, esperaMs = 45_000) {
  let b = null;
  const t0 = Date.now();
  try {
    b = await chromium.launch({ headless: HEADLESS, args: ARGS, ...(proxy ? { proxy: { server: proxy } } : {}) });
    const ctx = await b.newContext(CTX);
    await ctx.addInitScript(() => { Object.defineProperty(navigator, 'webdriver', { get: () => undefined }); });
    const p = await ctx.newPage();
    await p.goto(PAGINA, { waitUntil: 'domcontentloaded', timeout: 30_000 });
    let titulo = await p.title().catch(() => '');
    while (bloqueado(titulo) && Date.now() - t0 < esperaMs) { await dormir(2500); titulo = await p.title().catch(() => ''); }
    if (bloqueado(titulo)) return { ok: false, motivo: `Cloudflare («${titulo.slice(0, 40)}»)`, ms: Date.now() - t0 };
    const campus = await p.evaluate(() => new Promise((res) => {
      const x = new XMLHttpRequest();
      x.open('POST', 'Default.aspx/getCampus', true);
      x.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
      x.timeout = 20_000;
      x.onload = () => { try { const d = JSON.parse(x.responseText).d; res(Array.isArray(d) ? d.length : (typeof d === 'string' ? JSON.parse(d).length : -1)); } catch { res(-2); } };
      x.onerror = x.ontimeout = () => res(-3);
      x.send('{}');
    }));
    return { ok: campus > 0, motivo: campus > 0 ? `${campus} recintos leídos` : `la página abrió pero la consulta devolvió ${campus}`, ms: Date.now() - t0 };
  } catch (e) {
    return { ok: false, motivo: String(e.message || e).split('\n')[0].slice(0, 90), ms: Date.now() - t0 };
  } finally { try { await b?.close(); } catch { /* nada */ } }
}

const ip = await fetch('https://api.ipify.org').then((r) => r.text()).catch(() => '?');
console.log(`IP de salida del runner: ${ip}`);

const directo = await probar(null);
console.log(`Salida directa: ${directo.ok ? 'PASA' : 'NO pasa'} — ${directo.motivo} (${Math.round(directo.ms / 1000)} s)`);

const { lista, conteo } = await listaDeProxies();
console.log(`Proxies candidatos: ${lista.length}`, JSON.stringify(conteo));
const t = Date.now();
const vivos = await prefiltrar(lista, { quiero: 30 });
console.log(`Prefiltro (túnel hasta la UASD:443): ${vivos.length} vivos en ${Math.round((Date.now() - t) / 1000)} s`);

let pasan = 0;
const aProbar = vivos.slice(0, Number(process.env.DIAG_MAX || 15));
for (let i = 0; i < aProbar.length; i += 3) {
  const tanda = aProbar.slice(i, i + 3);
  const res = await Promise.all(tanda.map((p) => probar(p.url, 35_000)));
  res.forEach((r, k) => {
    if (r.ok) pasan++;
    console.log(`  ${r.ok ? 'PASA ' : 'falla'} ${tanda[k].fuente.padEnd(22)} ${r.motivo} (${Math.round(r.ms / 1000)} s)`);
  });
}
console.log(`\nResumen: directa ${directo.ok ? 'sí' : 'no'} · proxies que pasan ${pasan}/${aProbar.length}`);
