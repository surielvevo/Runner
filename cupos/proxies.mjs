// ─────────────────────────────────────────────────────────────────────────────
// proxies.mjs — Pool de proxies públicos para el scraper de cupos (2026-11-10)
// ─────────────────────────────────────────────────────────────────────────────
// Junta listas abiertas (cada fuente con su crédito en el README), deduplica y
// las ordena: primero las medidas con uptime alto (api.proxyscraper.com), luego
// el resto barajado. Antes de abrir un navegador por un proxy, un PREFILTRO de
// red muy barato (apretón de manos CONNECT/SOCKS5 hasta app.uasd.edu.do:443)
// descarta en paralelo los muertos: de cientos de candidatos quedan decenas, y
// solo esos pasan a la prueba cara (Chromium + Cloudflare).
//
// Seguridad: por el proxy solo viaja HTTPS de punta a punta (el navegador
// verifica el certificado; el proxy no ve ni puede alterar el contenido). Las
// llamadas a Supabase NUNCA salen por proxy.
// ─────────────────────────────────────────────────────────────────────────────

import net from 'node:net';

const HOST = 'app.uasd.edu.do';

/** Fuentes abiertas. `{k}` = clave gratuita opcional de api.proxyscraper.com. */
function fuentes(clave) {
  const ps = clave ? `https://api.proxyscraper.com/k/${encodeURIComponent(clave)}/v1` : 'https://api.proxyscraper.com/v1';
  return [
    // Medidas cada 10 min, con uptime ≥ 50 % en 24 h (las que más duran) y latencia razonable.
    { url: `${ps}/http.txt?min_uptime=50&max_latency=1500`, tipo: 'http', prioridad: 3, nombre: 'proxyscraper' },
    { url: `${ps}/socks5.txt?min_uptime=50&max_latency=1500`, tipo: 'socks5', prioridad: 3, nombre: 'proxyscraper' },
    { url: `${ps}/http/stable.txt`, tipo: 'http', prioridad: 3, nombre: 'proxyscraper-estables' },
    { url: `${ps}/http/do.txt`, tipo: 'http', prioridad: 4, nombre: 'proxyscraper-RD' },
    { url: 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt', tipo: 'http', prioridad: 1, nombre: 'proxifly' },
    { url: 'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt', tipo: 'socks5', prioridad: 1, nombre: 'proxifly' },
    { url: 'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text', tipo: 'auto', prioridad: 1, nombre: 'proxyscrape' },
    { url: 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt', tipo: 'http', prioridad: 0, nombre: 'TheSpeedX' },
    { url: 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt', tipo: 'socks5', prioridad: 0, nombre: 'TheSpeedX' },
    { url: 'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt', tipo: 'http', prioridad: 0, nombre: 'monosans' },
  ];
}

async function bajar(url, ms = 12_000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, { signal: ctrl.signal, headers: { 'User-Agent': 'cupos-uasd-runner' } });
    return r.ok ? await r.text() : '';
  } catch { return ''; } finally { clearTimeout(t); }
}

/** Lista deduplicada de proxies `{ url, prioridad, fuente }`, la mejor primero. */
export async function listaDeProxies({ clave = process.env.PROXY_SCRAPER_KEY } = {}) {
  const vistos = new Map();
  const textos = await Promise.all(fuentes(clave).map(async (f) => ({ f, txt: await bajar(f.url) })));
  const conteo = {};
  for (const { f, txt } of textos) {
    let n = 0;
    for (const linea of txt.split(/\s+/)) {
      const m = linea.trim().match(/^(?:(https?|socks5h?|socks4):\/\/)?(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$/i);
      if (!m) continue;
      const proto = (m[1] || (f.tipo === 'auto' ? 'http' : f.tipo)).toLowerCase().replace('socks5h', 'socks5');
      if (proto === 'socks4' || proto === 'https') continue; // Chromium: http y socks5
      const url = `${proto}://${m[2]}:${m[3]}`;
      const ya = vistos.get(url);
      if (!ya || ya.prioridad < f.prioridad) vistos.set(url, { url, prioridad: f.prioridad, fuente: f.nombre });
      n++;
    }
    conteo[f.nombre] = (conteo[f.nombre] || 0) + n;
  }
  const lista = [...vistos.values()];
  // Barajar dentro de cada prioridad (no pegarle siempre a los mismos) y ordenar por prioridad.
  for (let i = lista.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [lista[i], lista[j]] = [lista[j], lista[i]]; }
  lista.sort((a, b) => b.prioridad - a.prioridad);
  return { lista, conteo };
}

/** ¿El proxy abre un túnel TCP hasta la UASD:443? (CONNECT o SOCKS5, sin TLS ni navegador). */
export function alcanzaUasd(proxyUrl, ms = 6000) {
  return new Promise((resolve) => {
    let u;
    try { u = new URL(proxyUrl); } catch { resolve(false); return; }
    const s = net.connect({ host: u.hostname, port: Number(u.port) });
    let fase = 0;
    let buf = Buffer.alloc(0);
    const fin = (ok) => { clearTimeout(t); s.destroy(); resolve(ok); };
    const t = setTimeout(() => fin(false), ms);
    s.on('error', () => fin(false));
    s.on('connect', () => {
      if (u.protocol === 'http:') s.write(`CONNECT ${HOST}:443 HTTP/1.1\r\nHost: ${HOST}:443\r\n\r\n`);
      else s.write(Buffer.from([5, 1, 0]));
    });
    s.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      if (u.protocol === 'http:') {
        const txt = buf.toString('latin1');
        if (txt.includes('\r\n\r\n')) fin(/^HTTP\/1\.[01] 200/.test(txt));
        return;
      }
      if (fase === 0) {
        if (buf.length < 2) return;
        if (buf[0] !== 5 || buf[1] !== 0) return fin(false);
        fase = 1; buf = Buffer.alloc(0);
        const h = Buffer.from(HOST);
        s.write(Buffer.concat([Buffer.from([5, 1, 0, 3, h.length]), h, Buffer.from([1, 187])]));
        return;
      }
      if (buf.length >= 2) fin(buf[0] === 5 && buf[1] === 0);
    });
  });
}

/** Prefiltro en paralelo: devuelve hasta `quiero` proxies vivos, en el orden de entrada. */
export async function prefiltrar(lista, { quiero = 40, concurrencia = 64, ms = 6000, hastaMs = 90_000 } = {}) {
  const vivos = [];
  const fin = Date.now() + hastaMs;
  let i = 0;
  async function trabajador() {
    while (i < lista.length && vivos.length < quiero && Date.now() < fin) {
      const p = lista[i++];
      if (await alcanzaUasd(p.url, ms)) vivos.push(p);
    }
  }
  await Promise.all(Array.from({ length: concurrencia }, trabajador));
  return vivos.slice(0, quiero);
}
