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
// (2026-11-21) El 21-nov la UASD respondió 429 pidiendo ≥ 15 min: se respeta entero (hasta 1 h).
const RETRY_AFTER_MAX_MS = 60 * 60_000;
const RITMO_VIGENCIA_MS = 6 * 3_600_000;
// Umbral aprendido (como el ssthresh de TCP): el escalón que provocó un castigo largo no se vuelve
// a pisar en estas horas. Sin él, el ritmo subía, la UASD castigaba 15 min, bajaba… en bucle.
const UMBRAL_HORAS = 6;
const CASTIGO_LARGO_MS = 10 * 60_000;
// (2026-11-22) Medido: al caducar el veto, las 3 partes probaron a la vez el escalón vedado y la UASD
// las castigó juntas 1 h (sin lecturas de 02:52 a 03:52 UTC). Si el castigo llega poco después de que
// caducara el veto de ese mismo escalón, el veto siguiente dura el doble: 6 → 12 → 24 h.
const UMBRAL_MAX_HORAS = 24;
const REINCIDE_MS = 3 * 3_600_000;

const entero = (v, d) => (Number.isInteger(v) ? v : d);
const acotar = (v, min, max) => Math.min(max, Math.max(min, v));
/** Escalón más rápido permitido ahora: el tope configurado o el umbral aprendido, el más lento. */
const topeVigente = (r, t, ahora) =>
  (Number.isInteger(r.umbral) && (Number(r.umbralHasta) || 0) > ahora) ? Math.max(t, r.umbral) : t;
/** Veto ya caducado que aún delata una reincidencia (caducó hace menos de 3 h), o null. */
function vetoPrevio(r, ahora) {
  const p = Number.isInteger(r?.umbral) && (Number(r.umbralHasta) || 0) <= ahora
    ? { umbral: r.umbral, hasta: Number(r.umbralHasta) || 0, horas: Number(r.umbralHoras) || UMBRAL_HORAS }
    : r?.previo;
  return p && Number.isInteger(p.umbral) && ahora - (Number(p.hasta) || 0) < REINCIDE_MS
    ? { umbral: p.umbral, hasta: Number(p.hasta) || 0, horas: Number(p.horas) || UMBRAL_HORAS } : null;
}

/** Ritmo con que arranca un ciclo: el aprendido en el anterior (si es reciente) o el prudente. */
export function ritmoInicial(guardado, { tope = 1, arranque = 4, ahora = Date.now() } = {}) {
  const t = acotar(entero(tope, 1), 0, NIVEL_LENTO);
  let nivel = acotar(entero(arranque, 4), t, NIVEL_LENTO);
  let umbral = null, umbralHasta = 0, umbralHoras = 0;
  if (guardado && Number.isInteger(guardado.umbral) && (Number(guardado.umbralHasta) || 0) > ahora) {
    umbral = acotar(guardado.umbral, t, NIVEL_LENTO);
    umbralHasta = Number(guardado.umbralHasta);
    umbralHoras = Number(guardado.umbralHoras) || UMBRAL_HORAS;
  }
  if (guardado && Number.isInteger(guardado.nivel) && ahora - (Number(guardado.t) || 0) < RITMO_VIGENCIA_MS) {
    nivel = acotar(guardado.nivel, t, NIVEL_LENTO);
  }
  if (umbral !== null) nivel = Math.max(nivel, umbral);
  return { nivel, buenos: 0, malos: 0, umbral, umbralHasta, umbralHoras, previo: guardado ? vetoPrevio(guardado, ahora) : null };
}

/**
 * Ajusta el ritmo tras un lote. `lote`: { intentadas, ok, frenos, bloqueos, retryAfterS }.
 *  · Freno (≥ 20 % de frenos/bloqueos, o 2+ en un lote cortito): baja 1 escalón (2 si casi
 *    todo vino frenado) y pausa 30 s, 60 s, 120 s… (máx. 10 min; respeta Retry-After, hasta 1 h).
 *    Si el castigo es largo (Retry-After ≥ 10 min) o se repite, el escalón que lo provocó queda
 *    vedado 6 h (umbral): no se vuelve a subir hasta él. Si lo castigan otra vez nada más
 *    caducar su veto, el nuevo dura el doble (12 h, 24 h). `desfaseMs` (uno por parte) evita
 *    que las tres partes vuelvan a probar a la vez.
 *  · Lote limpio: tras 3 seguidos sube 1 escalón, nunca por encima del tope ni del umbral.
 *  · Algo de ruido sin freno: se mantiene.
 */
export function ajustarRitmo(r, lote, { tope = 1, ahora = Date.now(), desfaseMs = 0 } = {}) {
  const t = acotar(entero(tope, 1), 0, NIVEL_LENTO);
  const intentadas = Math.max(0, lote.intentadas | 0);
  const malas = Math.max(0, (lote.frenos | 0) + (lote.bloqueos | 0));
  const ok = Math.max(0, lote.ok | 0);
  const vigente = topeVigente(r, t, ahora);
  const umbral = vigente > t ? r.umbral : null;
  const umbralHasta = umbral !== null ? r.umbralHasta : 0;
  const umbralHoras = umbral !== null ? (Number(r.umbralHoras) || UMBRAL_HORAS) : 0;
  const previo = umbral !== null ? null : vetoPrevio(r, ahora);
  const frenado = malas > 0 && (intentadas === 0 || malas / intentadas >= 0.2 || (intentadas < 5 && malas >= 2));
  if (frenado) {
    const salto = intentadas > 0 && malas / intentadas >= 0.8 ? 2 : 1;
    const malos = r.malos + 1;
    const pausa = Math.min(PAUSA_MAX_MS, PAUSA_BASE_MS * 2 ** Math.min(malos - 1, 10));
    const pide = Math.min(RETRY_AFTER_MAX_MS, Math.max(0, Number(lote.retryAfterS) || 0) * 1000);
    const desde = Math.max(r.nivel, vigente);
    let nuevoUmbral = umbral, nuevoHasta = umbralHasta, nuevoHoras = umbralHoras, nuevoPrevio = previo;
    if (pide >= CASTIGO_LARGO_MS || malos >= 2) {
      nuevoUmbral = Math.min(NIVEL_LENTO, Math.max(umbral ?? t, desde + 1));
      // Reincidencia: el veto de este escalón (o de uno más lento) acababa de caducar.
      const reincide = previo !== null && nuevoUmbral <= previo.umbral;
      nuevoHoras = reincide ? Math.min(UMBRAL_MAX_HORAS, previo.horas * 2) : UMBRAL_HORAS;
      nuevoHasta = ahora + nuevoHoras * 3_600_000 + Math.max(0, Number(desfaseMs) || 0);
      nuevoPrevio = null;
    }
    return {
      ritmo: {
        nivel: Math.min(NIVEL_LENTO, desde + salto), buenos: 0, malos,
        umbral: nuevoUmbral, umbralHasta: nuevoHasta, umbralHoras: nuevoHoras, previo: nuevoPrevio,
      },
      pausaMs: Math.max(pausa, pide),
      motivo: 'freno',
    };
  }
  if (intentadas > 0 && malas === 0 && ok === intentadas) {
    const buenos = r.buenos + 1;
    if (buenos >= LOTES_SANOS_PARA_SUBIR && r.nivel > vigente) {
      return { ritmo: { nivel: r.nivel - 1, buenos: 0, malos: 0, umbral, umbralHasta, umbralHoras, previo }, pausaMs: 0, motivo: 'sube' };
    }
    return { ritmo: { nivel: Math.max(r.nivel, vigente), buenos, malos: 0, umbral, umbralHasta, umbralHoras, previo }, pausaMs: 0, motivo: 'sano' };
  }
  return { ritmo: { nivel: Math.max(r.nivel, vigente), buenos: 0, malos: 0, umbral, umbralHasta, umbralHoras, previo }, pausaMs: 0, motivo: 'estable' };
}

// ── 2. Cada cuánto releer una materia ─────────────────────────────────────────
// (2026-11-23, reglas del dueño para la selección) Se decide POR SECCIÓN. La UASD responde por
// materia (una consulta trae todas sus secciones), así que manda su sección ABIERTA con menos
// cupos; las llenas solo cuentan para cazar el cupo que se libere. Vigilancia activa desde 10 cupos
// y más intensa desde 5; de 11 a 20 todavía hay cupos: solo revisión periódica; con más de 20 no se
// toca (barrido diario, y el carril de demanda la relee al momento si un estudiante la abre).
// Lo que no cambia se relee cada vez menos (×2 por lectura igual, hasta el techo) y un cambio la
// devuelve a la base. Cuando el caudal no alcanza, la cola va por atraso (edad / intervalo): todas
// las clases avanzan en proporción y ninguna se queda sin leer (sin huecos).
export const CLASES = Object.freeze({
  //                     base: horas × PRIORITY_SPEED (la de vigilada es fija) · techo: horas
  vigilada:     Object.freeze({ base: 0.025, techo: 0.1 }), // agente de cupos y apretada: 1,5–6 min, al frente
  critica:      Object.freeze({ base: 0.1, techo: 0.25 }),  // 1–5 cupos: 3–15 min
  activa:       Object.freeze({ base: 0.25, techo: 0.5 }),  // 6–10 cupos (o llena sin detalle): 7,5–30 min
  agotada:      Object.freeze({ base: 0.25, techo: 0.5 }),  // TODAS llenas: ahí la UASD abre secciones nuevas
  nuevaSeccion: Object.freeze({ base: 0.25, techo: 0.5 }),  // estrenó sección (NRC nuevo) hace < 6 h
  caza:         Object.freeze({ base: 2, techo: 2 }),       // solo quedan llenas (o llenas + holgadas): 1–2 h
  periodica:    Object.freeze({ base: 3, techo: 4 }),       // 11–20 cupos: 1,5–4 h
  holgada:      Object.freeze({ base: 24, techo: 24 }),     // > 20 cupos en todas: 12–24 h
  sinSecciones: Object.freeze({ base: 12, techo: 12 }),     // la UASD no lista ninguna: 6–12 h (¿abrió la primera?)
});

/**
 * Lo que importa de las secciones de una materia según la memoria de su última lectura
 * (`d` = { nrc: disponibles }, `m` = menor disponible, 0 si hay alguna llena u oculta).
 * Devuelve { minAbierta, hayLlena } o null si no hay detalle por sección.
 */
export function resumenSecciones(mem) {
  if (!mem || !mem.d || typeof mem.d !== 'object') return null;
  let minAbierta = Infinity, hayLlena = Number(mem.m) <= 0;
  for (const v of Object.values(mem.d)) {
    const n = Number(v) || 0;
    if (n > 0) { if (n < minAbierta) minAbierta = n; } else hayLlena = true;
  }
  return { minAbierta, hayLlena };
}

/** Ventana en que una materia que estrenó sección sigue en vigilancia activa (la UASD suele abrir varias). */
export const NUEVA_SECCION_MS = 6 * 3_600_000;

/**
 * Clase de relectura de una materia (ver CLASES). `minAbierta` = Infinity si no queda ninguna abierta.
 * `recienNueva` = apareció una sección (NRC) que no estaba en la lectura anterior hace menos de 6 h.
 */
export function claseRelectura({ minAbierta = Infinity, hayLlena = false, nSecc = 1, vigilada = false, sinDetalle = false, recienNueva = false }) {
  if (!(nSecc > 0)) return 'sinSecciones';
  const m = Number.isFinite(minAbierta) ? minAbierta : Infinity;
  if (vigilada && (hayLlena || m <= 10)) return 'vigilada';
  if (m <= 5) return 'critica';
  if (m <= 10) return 'activa';
  // Llena sin detalle por sección (solo el mínimo de la base): podría tener una abierta casi
  // agotada detrás del 0, así que va como activa hasta la primera lectura (sin huecos).
  if (hayLlena && sinDetalle) return 'activa';
  if (hayLlena && m === Infinity) return 'agotada';
  if (recienNueva) return 'nuevaSeccion';
  if (hayLlena) return 'caza';
  if (m <= 20) return 'periodica';
  return 'holgada';
}

/** Intervalo de relectura (horas) de una clase: base × 2 por lectura sin cambios, hasta el techo. */
export function intervaloRelectura(clase, { sinCambios = 0, speed = 1 } = {}) {
  const c = CLASES[clase] || CLASES.holgada;
  const b = Math.max(0.005, clase === 'vigilada' ? c.base : c.base * (Number(speed) || 1));
  const mult = 2 ** Math.min(Math.max(0, sinCambios | 0), 4);
  return Math.max(Math.min(b, c.techo), Math.min(b * mult, c.techo));
}

/**
 * (2026-11-23) ¿Se da por leída una respuesta de la UASD que trae filas pero NINGUNA sección válida
 * (sin periodo ni NRC)? Sí cuando la materia nunca tuvo secciones: es su forma de decir «no hay».
 * Antes no se daba por leída y seguía «nueva»: ~106 materias por parte se releían en CADA ciclo
 * (dos tercios del caudal, medido el 23-nov). Si la materia sí tenía secciones, no: puede ser una
 * respuesta a medias y se vuelve a pedir (y nunca cierra secciones).
 */
export function respuestaSinSecciones({ filas = 0, validas = 0, seccionesConocidas = 0 }) {
  return filas > 0 && validas === 0 && !(seccionesConocidas > 0);
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
  // (2026-11-23) Secciones (NRC) que no estaban en la lectura anterior: sección nueva o una cerrada que
  // reapareció. Sin lectura anterior no se cuenta nada (no es «nueva», es la primera vez que se mira).
  let nuevos = 0;
  if (!cambio) {
    for (const [nrc, d] of Object.entries(ahora)) {
      if (!(nrc in anterior)) nuevos++;
      if (anterior[nrc] !== d) cambio = true;
    }
  }
  const mapa = { ...ahora };
  if (anterior && typeof anterior === 'object') {
    for (const nrc of pendientes) if (!(nrc in mapa) && nrc in anterior) mapa[nrc] = anterior[nrc];
  }
  return { cambio, mapa, nuevos };
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
