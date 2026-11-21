// ─────────────────────────────────────────────────────────────────────────────
// ritmo.mjs — El «cerebro» del ETL de cupos: cuánto pedir, cuándo y qué creer (2026-11-21)
// ─────────────────────────────────────────────────────────────────────────────
// Lógica PURA (sin red, sin navegador, sin base): se prueba con `npm test`.
//
// Por qué existe (incidente del 16 al 21-nov-2026): el ETL pedía ~900.000 lecturas al día
// (cada materia pendiente se releía ~40 veces por ciclo) y la UASD empezó a frenar `getData`
// (40 de 40 consultas frenadas) y a vetar algunas IP con su propia página «403 Prohibido».
// El ETL no lo veía: reintentaba en ráfaga, tomaba el veto por «Cloudflare superado» y el
// turno vetado se relanzaba cada 35 s (570 veces). Cuatro piezas para que no vuelva a pasar:
//   1. Ritmo AIMD (como el control de congestión de TCP): baja a la mitad al primer freno,
//      sube un escalón tras varios lotes sanos. La UASD decide cuánto aguanta, no nosotros.
//   2. Lectura según cuánto cambia cada materia: lo que no cambia se relee cada vez menos
//      (×2 por lectura igual, con techo de edad); lo que se mueve vuelve al ritmo corto.
//   3. Doble confirmación antes de cerrar una sección: la UASD a veces omite campus enteros
//      en una respuesta (31.000 cierres falsos al día entre el 12 y el 16-nov).
//   4. Veto del servidor reconocido como bloqueo (no como éxito ni como error suelto).
// ─────────────────────────────────────────────────────────────────────────────

// ── 1. Ritmo AIMD ────────────────────────────────────────────────────────────
// Escalones del más rápido (0) al más lento. Rápido = el ritmo de antes del 16-nov.
export const NIVELES = Object.freeze([
  Object.freeze({ conc: 3, gapMin: 400, gapMax: 900 }),      // 0 · el de siempre
  Object.freeze({ conc: 2, gapMin: 900, gapMax: 1500 }),     // 1 · tope por defecto
  Object.freeze({ conc: 1, gapMin: 1500, gapMax: 2500 }),    // 2
  Object.freeze({ conc: 1, gapMin: 3000, gapMax: 4500 }),    // 3 · arranque prudente
  Object.freeze({ conc: 1, gapMin: 6000, gapMax: 9000 }),    // 4
  Object.freeze({ conc: 1, gapMin: 12000, gapMax: 18000 }),  // 5
  Object.freeze({ conc: 1, gapMin: 30000, gapMax: 45000 }),  // 6 · al paso
]);
export const NIVEL_LENTO = NIVELES.length - 1;
const LOTES_SANOS_PARA_SUBIR = 3;
const PAUSA_BASE_MS = 30_000;
const PAUSA_MAX_MS = 10 * 60_000;
const RETRY_AFTER_MAX_MS = 15 * 60_000;
const RITMO_VIGENCIA_MS = 6 * 3_600_000;

const entero = (v, d) => (Number.isInteger(v) ? v : d);
const acotar = (v, min, max) => Math.min(max, Math.max(min, v));

/** Ritmo con que arranca un ciclo: el aprendido en el anterior (si es reciente) o el prudente. */
export function ritmoInicial(guardado, { tope = 1, arranque = 3, ahora = Date.now() } = {}) {
  const t = acotar(entero(tope, 1), 0, NIVEL_LENTO);
  let nivel = acotar(entero(arranque, 3), t, NIVEL_LENTO);
  if (guardado && Number.isInteger(guardado.nivel) && ahora - (Number(guardado.t) || 0) < RITMO_VIGENCIA_MS) {
    nivel = acotar(guardado.nivel, t, NIVEL_LENTO);
  }
  return { nivel, buenos: 0, malos: 0 };
}

/**
 * Ajusta el ritmo tras un lote. `lote`: { intentadas, ok, frenos, bloqueos, retryAfterS }.
 *  · Freno (≥ 20 % de frenos/bloqueos, o 2+ en un lote cortito): baja 1 escalón (2 si casi
 *    todo vino frenado) y pausa 30 s, 60 s, 120 s… (máx. 10 min; respeta Retry-After).
 *  · Lote limpio: tras 3 seguidos sube 1 escalón, nunca por encima del tope.
 *  · Algo de ruido sin freno: se mantiene.
 */
export function ajustarRitmo(r, lote, { tope = 1 } = {}) {
  const t = acotar(entero(tope, 1), 0, NIVEL_LENTO);
  const intentadas = Math.max(0, lote.intentadas | 0);
  const malas = Math.max(0, (lote.frenos | 0) + (lote.bloqueos | 0));
  const ok = Math.max(0, lote.ok | 0);
  const frenado = malas > 0 && (intentadas === 0 || malas / intentadas >= 0.2 || (intentadas < 5 && malas >= 2));
  if (frenado) {
    const salto = intentadas > 0 && malas / intentadas >= 0.8 ? 2 : 1;
    const malos = r.malos + 1;
    const pausa = Math.min(PAUSA_MAX_MS, PAUSA_BASE_MS * 2 ** Math.min(malos - 1, 10));
    const pide = Math.min(RETRY_AFTER_MAX_MS, Math.max(0, Number(lote.retryAfterS) || 0) * 1000);
    return {
      ritmo: { nivel: Math.min(NIVEL_LENTO, Math.max(r.nivel, t) + salto), buenos: 0, malos },
      pausaMs: Math.max(pausa, pide),
      motivo: 'freno',
    };
  }
  if (intentadas > 0 && malas === 0 && ok === intentadas) {
    const buenos = r.buenos + 1;
    if (buenos >= LOTES_SANOS_PARA_SUBIR && r.nivel > t) {
      return { ritmo: { nivel: r.nivel - 1, buenos: 0, malos: 0 }, pausaMs: 0, motivo: 'sube' };
    }
    return { ritmo: { nivel: Math.max(r.nivel, t), buenos, malos: 0 }, pausaMs: 0, motivo: 'sano' };
  }
  return { ritmo: { nivel: Math.max(r.nivel, t), buenos: 0, malos: 0 }, pausaMs: 0, motivo: 'estable' };
}

// ── 2. Cada cuánto releer una materia ─────────────────────────────────────────
/** Edad máxima (horas) que se tolera según el tipo de materia, aunque no cambie nunca. */
export function techoHoras({ minDisp, fija = false, nSecc = 1 }) {
  if (fija) return 0.1;          // vigilada por el agente de cupos: como mucho 6 min
  if (nSecc === 0) return 24;    // la UASD no lista ninguna sección: una vez al día
  if (minDisp <= 10) return 0.5; // apretada: 30 min
  if (minDisp <= 24) return 2;   // bajando: 2 h
  return 8;                      // con cupos de sobra: 8 h (el barrido ligero de siempre)
}

/**
 * Intervalo de relectura (horas). `base` = presupuesto de siempre por cupos (ya con
 * PRIORITY_SPEED). Cada lectura sin cambios lo duplica (×2, ×4, ×8, ×16) hasta el techo;
 * un cambio lo devuelve a la base (`sinCambios` = 0).
 */
export function intervaloHoras({ base, minDisp, fija = false, sinCambios = 0, nSecc = 1 }) {
  const techo = techoHoras({ minDisp, fija, nSecc });
  const b = Math.max(0.005, Number(base) || 0);
  const mult = 2 ** Math.min(Math.max(0, sinCambios | 0), 4);
  return Math.max(Math.min(b, techo), Math.min(b * mult, techo));
}

// ── 3. Qué creer de una respuesta ─────────────────────────────────────────────
/**
 * ¿Cambió la materia respecto a la lectura anterior? `anterior` = { nrc: disponibles } o null.
 * Una sección que FALTA no cuenta como cambio por sí sola (puede ser una respuesta a medias);
 * cuenta cuando se confirma su cierre (lo decide `confirmarFaltantes`).
 * Devuelve el mapa para la próxima comparación: lo leído + lo que falta sin confirmar.
 */
export function compararLectura(anterior, filas, pendientes = []) {
  const ahora = {};
  for (const f of filas) if (f && f.nrc) ahora[String(f.nrc)] = Number(f.disponibles) || 0;
  let cambio = !anterior || typeof anterior !== 'object';
  if (!cambio) {
    for (const [nrc, d] of Object.entries(ahora)) {
      if (anterior[nrc] !== d) { cambio = true; break; }
    }
  }
  const mapa = { ...ahora };
  if (anterior && typeof anterior === 'object') {
    for (const nrc of pendientes) if (!(nrc in mapa) && nrc in anterior) mapa[nrc] = anterior[nrc];
  }
  return { cambio, mapa };
}

/**
 * Doble confirmación de cierres. `faltabanAntes`: claves («periodo|nrc») que ya faltaron en la
 * lectura anterior de la materia; `faltanAhora`: las que faltan en esta. Se cierra solo lo que
 * faltó DOS lecturas seguidas; lo demás queda pendiente para la próxima.
 */
export function confirmarFaltantes(faltabanAntes, faltanAhora) {
  const antes = faltabanAntes instanceof Set ? faltabanAntes : new Set(faltabanAntes || []);
  const cerrar = [];
  const pendientes = [];
  for (const k of new Set(faltanAhora || [])) (antes.has(k) ? cerrar : pendientes).push(k);
  return { cerrar, pendientes };
}

/** Periodo dominante de una respuesta (el de más secciones). */
export function periodoDominante(filas) {
  const cuenta = new Map();
  for (const f of filas) if (f && f.periodo) cuenta.set(f.periodo, (cuenta.get(f.periodo) || 0) + 1);
  let mejor = null, n = -1;
  for (const [p, c] of cuenta) if (c > n) { mejor = p; n = c; }
  return mejor;
}

// ── 4. Reconocer bloqueos ─────────────────────────────────────────────────────
/** Título de página de bloqueo DURO: Cloudflare 1020 o el veto propio de la UASD («403 Prohibido»). */
export function esBloqueoDuro(titulo) {
  const s = String(titulo || '').toLowerCase();
  return /access denied|acceso denegado|restrict access|\b1020\b|\b403\b|prohibido|forbidden/.test(s);
}

/** ¿El error de un ciclo es un bloqueo/freno de la UASD (condición pasajera, no un bug)? */
export function esErrorDeBloqueo(mensaje) {
  return /IP_BLOQUEADA|No se pudo pasar Cloudflare|rate-limit|penalizaci|access denied|HTTP 403|isHTML=true/i
    .test(String(mensaje || ''));
}

/** Escalera de descanso de una IP bloqueada (min), según cuántos bloqueos seguidos lleva. */
export function descansoMin(racha) {
  return [0, 3, 8, 15, 25, 35, 45][Math.min(Math.max(0, racha | 0), 6)];
}
