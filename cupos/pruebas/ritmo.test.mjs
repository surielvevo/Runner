// Pruebas de ritmo.mjs (sin red). Correr: npm test (desde cupos/).
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  NIVELES, NIVEL_LENTO, ritmoInicial, ajustarRitmo,
  CLASES, resumenSecciones, claseRelectura, intervaloRelectura, respuestaSinSecciones, NUEVA_SECCION_MS,
  compararLectura, confirmarFaltantes, periodoDominante, esBloqueoDuro, esErrorDeBloqueo, descansoMin,
} from '../ritmo.mjs';

const lote = (intentadas, ok, frenos = 0, bloqueos = 0, retryAfterS = 0) => ({ intentadas, ok, frenos, bloqueos, retryAfterS });

test('arranca prudente sin memoria y respeta el ritmo aprendido reciente', () => {
  assert.equal(ritmoInicial(null).nivel, 4);
  const ahora = 1_000_000_000_000;
  assert.equal(ritmoInicial({ nivel: 5, t: ahora - 60_000 }, { ahora }).nivel, 5);
  // Memoria de hace más de 6 h: se ignora.
  assert.equal(ritmoInicial({ nivel: 6, t: ahora - 7 * 3_600_000 }, { ahora }).nivel, 4);
  // Nunca más rápido que el tope.
  assert.equal(ritmoInicial({ nivel: 0, t: ahora }, { ahora, tope: 1 }).nivel, 1);
  assert.equal(ritmoInicial({ nivel: 0, t: ahora }, { ahora, tope: 0 }).nivel, 0);
});

test('el primer freno baja el ritmo y pausa; los siguientes doblan la pausa', () => {
  let r = { nivel: 1, buenos: 0, malos: 0 };
  let a = ajustarRitmo(r, lote(10, 7, 3));
  assert.equal(a.motivo, 'freno');
  assert.equal(a.ritmo.nivel, 2);
  assert.equal(a.pausaMs, 30_000);
  a = ajustarRitmo(a.ritmo, lote(10, 0, 10));          // casi todo frenado: baja 2
  assert.equal(a.ritmo.nivel, 4);
  assert.equal(a.pausaMs, 60_000);
  a = ajustarRitmo(a.ritmo, lote(3, 0, 3));
  assert.equal(a.ritmo.nivel, NIVEL_LENTO);
  assert.equal(a.pausaMs, 120_000);
  // Nunca pasa del escalón más lento ni de 10 min de pausa.
  for (let i = 0; i < 20; i++) a = ajustarRitmo(a.ritmo, lote(2, 0, 2));
  assert.equal(a.ritmo.nivel, NIVEL_LENTO);
  assert.equal(a.pausaMs, 10 * 60_000);
});

test('respeta el Retry-After de la UASD (con tope de 1 h)', () => {
  const r = { nivel: 3, buenos: 0, malos: 0 };
  assert.equal(ajustarRitmo(r, lote(4, 0, 4, 0, 300)).pausaMs, 300_000);
  assert.equal(ajustarRitmo(r, lote(4, 0, 4, 0, 99_999)).pausaMs, 60 * 60_000);
});

test('un lote frenado sin ninguna consulta intentada también cuenta como freno', () => {
  const a = ajustarRitmo({ nivel: 3, buenos: 0, malos: 0 }, lote(0, 0, 3));
  assert.equal(a.motivo, 'freno');
  assert.ok(a.ritmo.nivel > 3);
});

test('sube un escalón tras 3 lotes limpios y nunca pasa el tope', () => {
  let r = { nivel: 4, buenos: 0, malos: 0 };
  r = ajustarRitmo(r, lote(10, 10)).ritmo;
  r = ajustarRitmo(r, lote(10, 10)).ritmo;
  assert.equal(r.nivel, 4);
  const a = ajustarRitmo(r, lote(10, 10));
  assert.equal(a.motivo, 'sube');
  assert.equal(a.ritmo.nivel, 3);
  let t = { nivel: 1, buenos: 0, malos: 0 };
  for (let i = 0; i < 10; i++) t = ajustarRitmo(t, lote(10, 10), { tope: 1 }).ritmo;
  assert.equal(t.nivel, 1);
});

test('ruido sin freno (errores sueltos) mantiene el ritmo y corta la racha buena', () => {
  const a = ajustarRitmo({ nivel: 2, buenos: 2, malos: 0 }, lote(40, 39, 1));
  assert.equal(a.motivo, 'estable');
  assert.equal(a.ritmo.nivel, 2);
  assert.equal(a.ritmo.buenos, 0);
  assert.equal(a.pausaMs, 0);
});

test('los escalones van de rápido a lento', () => {
  for (let i = 1; i < NIVELES.length; i++) {
    const a = NIVELES[i - 1], b = NIVELES[i];
    assert.ok(b.conc <= a.conc && b.gapMin >= a.gapMin && b.gapMax >= b.gapMin, `nivel ${i}`);
  }
});

test('compararLectura: igual = sin cambio; otro número o sección nueva = cambio', () => {
  const filas = [{ nrc: '1', disponibles: 5 }, { nrc: '2', disponibles: 0 }];
  assert.equal(compararLectura(null, filas).cambio, true);
  const { mapa } = compararLectura(null, filas);
  assert.equal(compararLectura(mapa, filas).cambio, false);
  assert.equal(compararLectura(mapa, [{ nrc: '1', disponibles: 4 }, { nrc: '2', disponibles: 0 }]).cambio, true);
  assert.equal(compararLectura(mapa, [...filas, { nrc: '3', disponibles: 9 }]).cambio, true);
});

test('compararLectura: una sección que falta no es cambio y se recuerda mientras está pendiente', () => {
  const antes = { 1: 5, 2: 7 };
  const r = compararLectura(antes, [{ nrc: '1', disponibles: 5 }], ['2']);
  assert.equal(r.cambio, false);
  assert.deepEqual(r.mapa, { 1: 5, 2: 7 });
  // Si reaparece igual, sigue sin cambio (era una respuesta a medias).
  assert.equal(compararLectura(r.mapa, [{ nrc: '1', disponibles: 5 }, { nrc: '2', disponibles: 7 }]).cambio, false);
});

test('confirmarFaltantes: solo se cierra lo que faltó dos lecturas seguidas', () => {
  let r = confirmarFaltantes(new Set(), ['P|1', 'P|2']);
  assert.deepEqual(r.cerrar, []);
  assert.deepEqual(r.pendientes.sort(), ['P|1', 'P|2']);
  r = confirmarFaltantes(new Set(r.pendientes), ['P|2', 'P|3']);
  assert.deepEqual(r.cerrar, ['P|2']);
  assert.deepEqual(r.pendientes, ['P|3']);
  // La que reapareció (P|1) ya no está pendiente.
  assert.ok(!r.pendientes.includes('P|1'));
});

test('periodoDominante elige el periodo con más secciones', () => {
  assert.equal(periodoDominante([{ periodo: 'A' }, { periodo: 'B' }, { periodo: 'B' }]), 'B');
  assert.equal(periodoDominante([]), null);
});

test('reconoce el veto de la UASD y los bloqueos de Cloudflare; no confunde la página buena', () => {
  assert.equal(esBloqueoDuro('UASD · 403 Prohibido por el servidor'), true);
  assert.equal(esBloqueoDuro('Access denied | app.uasd.edu.do used Cloudflare to restrict access'), true);
  assert.equal(esBloqueoDuro('Error 1020'), true);
  assert.equal(esBloqueoDuro('403 Forbidden'), true);
  assert.equal(esBloqueoDuro('Programacion Docente Por Asignatura | UASD'), false);
  assert.equal(esBloqueoDuro(''), false);
});

test('clasifica como bloqueo el 403 de getCampus (antes salía como error y se relanzaba cada 35 s)', () => {
  assert.equal(esErrorDeBloqueo('page.evaluate: Error: HTTP 403 isHTML=true\n    at xhr.onload'), true);
  assert.equal(esErrorDeBloqueo('IP_BLOQUEADA::UASD · 403 Prohibido por el servidor'), true);
  assert.equal(esErrorDeBloqueo('No se pudo pasar Cloudflare (título="Un momento")'), true);
  assert.equal(esErrorDeBloqueo('TypeError: x is not a function'), false);
});

test('escalera de descanso de una IP bloqueada', () => {
  assert.deepEqual([0, 1, 2, 3, 4, 5, 6, 9].map(descansoMin), [0, 3, 8, 15, 25, 35, 45, 45]);
});

test('umbral: el escalón que provocó un castigo largo queda vedado 6 h', () => {
  const ahora = 1_000_000_000_000;
  // En el escalón 3 la UASD castiga 15 min: baja a 4 y el 3 queda vedado.
  let a = ajustarRitmo({ nivel: 3, buenos: 0, malos: 0 }, lote(6, 3, 3, 0, 900), { ahora });
  assert.equal(a.ritmo.nivel, 4);
  assert.equal(a.ritmo.umbral, 4);
  assert.equal(a.pausaMs, 900_000);
  // Muchos lotes limpios después: no vuelve al 3 mientras dure el umbral.
  let r = a.ritmo;
  for (let i = 0; i < 12; i++) r = ajustarRitmo(r, lote(10, 10), { ahora: ahora + 60_000 * i }).ritmo;
  assert.equal(r.nivel, 4);
  // Pasadas 6 h, puede volver a probar el 3 (y más rápido) con cuidado.
  const despues = ahora + 7 * 3_600_000;
  for (let i = 0; i < 3; i++) r = ajustarRitmo(r, lote(10, 10), { ahora: despues }).ritmo;
  assert.equal(r.nivel, 3);
});

test('umbral: un freno corto aislado no veda nada; dos seguidos sí', () => {
  const ahora = 1_000_000_000_000;
  let a = ajustarRitmo({ nivel: 2, buenos: 0, malos: 0 }, lote(10, 7, 3), { ahora });
  assert.equal(a.ritmo.umbral, null);
  a = ajustarRitmo(a.ritmo, lote(10, 7, 3), { ahora });
  assert.equal(a.ritmo.umbral, 4);           // se frenó en el 3 → vedado hasta el 4
  assert.equal(a.ritmo.nivel, 4);
});

test('el umbral aprendido viaja entre ciclos y caduca solo', () => {
  const ahora = 1_000_000_000_000;
  const guardado = { nivel: 2, t: ahora - 60_000, umbral: 5, umbralHasta: ahora + 3_600_000 };
  const r = ritmoInicial(guardado, { ahora });
  assert.equal(r.nivel, 5);
  assert.equal(r.umbral, 5);
  const caducado = ritmoInicial({ ...guardado, umbralHasta: ahora - 1 }, { ahora });
  assert.equal(caducado.nivel, 2);
  assert.equal(caducado.umbral, null);
});

test('umbral reincidente: castigado al caducar el veto, el siguiente dura el doble (tope 7 días)', () => {
  const H = 3_600_000;
  const t0 = 1_000_000_000_000;
  // Caso real del 22-nov: umbral 5 (6 h) caduca, sube al 4 tras 3 lotes limpios y la UASD castiga 1 h.
  let r = ritmoInicial({ nivel: 5, t: t0 - 60_000, umbral: 5, umbralHasta: t0 + 60_000, umbralHoras: 6 }, { ahora: t0 });
  assert.equal(r.umbral, 5);
  const t1 = t0 + 2 * 60_000;                      // el veto ya caducó
  for (let i = 0; i < 3; i++) r = ajustarRitmo(r, lote(10, 10), { ahora: t1 }).ritmo;
  assert.equal(r.nivel, 4);                        // prueba el escalón que estaba vedado
  assert.equal(r.umbral, null);
  assert.equal(r.previo.umbral, 5);                // pero recuerda el veto recién caducado
  let a = ajustarRitmo(r, lote(5, 2, 3, 0, 3600), { ahora: t1 + 60_000 });
  assert.equal(a.ritmo.umbral, 5);
  assert.equal(a.ritmo.umbralHoras, 12);
  assert.equal(a.ritmo.umbralHasta, t1 + 60_000 + 12 * H);
  assert.equal(a.pausaMs, H);
  // (2026-11-23) Cada reincidencia sigue doblando: 24, 48, 96 y 168 h (7 días), y no pasa de ahí.
  const esperadas = [24, 48, 96, 168, 168];
  for (const horas of esperadas) {
    const t = a.ritmo.umbralHasta + 60_000;
    a = ajustarRitmo({ ...a.ritmo, nivel: 4, malos: 0 }, lote(5, 2, 3, 0, 3600), { ahora: t });
    assert.equal(a.ritmo.umbral, 5);
    assert.equal(a.ritmo.umbralHoras, horas);
    assert.equal(a.ritmo.umbralHasta, t + horas * H);
  }
});

test('umbral: caso real del 23-nov, el veto de 24 h caduca, la prueba sale castigada y el siguiente dura 48 h', () => {
  const H = 3_600_000;
  const t0 = 1_000_000_000_000;
  // Parte 1 a las 20:56 UTC: «umbral 5 hasta 20:56 UTC (24 h, reincidió)» acaba de caducar.
  let r = ritmoInicial({ nivel: 5, t: t0 - 60_000, umbral: 5, umbralHasta: t0 - 1, umbralHoras: 24 }, { ahora: t0 });
  assert.equal(r.umbral, null);
  assert.equal(r.previo.horas, 24);
  for (let i = 0; i < 3; i++) r = ajustarRitmo(r, lote(4, 4), { ahora: t0 + i * 60_000 }).ritmo;
  assert.equal(r.nivel, 4);                        // prueba el escalón vedado…
  const a = ajustarRitmo(r, lote(4, 1, 3, 0, 2861), { ahora: t0 + 5 * 60_000 });
  assert.equal(a.ritmo.nivel, 5);                  // …la UASD castiga (429 + Retry-After 2861 s)
  assert.equal(a.ritmo.umbral, 5);
  assert.equal(a.ritmo.umbralHoras, 48);           // antes se quedaba en 24 h y se repetía cada día
  assert.equal(a.pausaMs, 2861 * 1000);
});

test('umbral: si el veto caducó hace más de 3 h, o el castigo es en un escalón más lento, vuelve a 6 h', () => {
  const H = 3_600_000;
  const t0 = 1_000_000_000_000;
  const caducado = { nivel: 4, buenos: 0, malos: 0, umbral: 5, umbralHasta: t0 - 4 * H, umbralHoras: 12 };
  let a = ajustarRitmo(caducado, lote(5, 2, 3, 0, 3600), { ahora: t0 });
  assert.equal(a.ritmo.umbralHoras, 6);
  // Veto 5 activo y castigo en el 5 (permitido): es un escalón nuevo, 6 h.
  a = ajustarRitmo({ nivel: 5, buenos: 0, malos: 0, umbral: 5, umbralHasta: t0 + H, umbralHoras: 12 }, lote(5, 2, 3, 0, 3600), { ahora: t0 });
  assert.equal(a.ritmo.umbral, 6);
  assert.equal(a.ritmo.umbralHoras, 6);
});

test('umbral: cada parte lo desfasa para no volver a probar las tres a la vez', () => {
  const t0 = 1_000_000_000_000;
  const base = ajustarRitmo({ nivel: 3, buenos: 0, malos: 0 }, lote(6, 3, 3, 0, 900), { ahora: t0 });
  const parte3 = ajustarRitmo({ nivel: 3, buenos: 0, malos: 0 }, lote(6, 3, 3, 0, 900), { ahora: t0, desfaseMs: 140 * 60_000 });
  assert.equal(parte3.ritmo.umbralHasta - base.ritmo.umbralHasta, 140 * 60_000);
  // El desfase solo mueve el momento de la prueba, no el ritmo de ahora.
  assert.equal(parte3.ritmo.nivel, base.ritmo.nivel);
});

test('el veto recién caducado viaja entre ciclos (y se olvida a las 3 h)', () => {
  const H = 3_600_000;
  const t0 = 1_000_000_000_000;
  const guardado = { nivel: 4, t: t0 - 60_000, umbral: null, umbralHasta: 0, previo: { umbral: 5, hasta: t0 - H, horas: 12 } };
  assert.deepEqual(ritmoInicial(guardado, { ahora: t0 }).previo, { umbral: 5, hasta: t0 - H, horas: 12 });
  assert.equal(ritmoInicial(guardado, { ahora: t0 + 3 * H }).previo, null);
});

test('respuesta con filas pero sin ninguna sección válida', () => {
  // Materia que nunca tuvo secciones: se da por leída (no vuelve en cada ciclo).
  assert.equal(respuestaSinSecciones({ filas: 1, validas: 0, seccionesConocidas: 0 }), true);
  // Materia con secciones: puede ser una respuesta a medias → se vuelve a pedir.
  assert.equal(respuestaSinSecciones({ filas: 1, validas: 0, seccionesConocidas: 4 }), false);
  // Lista vacía o con secciones válidas: no es este caso.
  assert.equal(respuestaSinSecciones({ filas: 0, validas: 0, seccionesConocidas: 0 }), false);
  assert.equal(respuestaSinSecciones({ filas: 3, validas: 3, seccionesConocidas: 0 }), false);
});

// (2026-11-23) Reglas del dueño para la selección: por sección, activa desde 10, intensa desde 5,
// 11–20 periódica, > 20 no se toca. Sustituye a techoHoras/intervaloHoras (techo 8 h a las holgadas).
test('clase por sección: manda la abierta con menos cupos', () => {
  const c = (minAbierta, extra = {}) => claseRelectura({ minAbierta, ...extra });
  assert.equal(c(1), 'critica');
  assert.equal(c(5), 'critica');
  assert.equal(c(6), 'activa');
  assert.equal(c(10), 'activa');
  assert.equal(c(11), 'periodica');
  assert.equal(c(15), 'periodica');   // con 15 todavía hay cupos: no es vigilancia activa
  assert.equal(c(20), 'periodica');
  assert.equal(c(21), 'holgada');     // > 20: no se toca
  assert.equal(c(Infinity, { hayLlena: true }), 'agotada');         // todas llenas: ahí abren secciones
  assert.equal(c(30, { hayLlena: true }), 'caza');                  // llena + holgadas
  assert.equal(c(4, { hayLlena: true }), 'critica');                // llena + una casi agotada
  assert.equal(c(Infinity, { hayLlena: true, sinDetalle: true }), 'activa'); // sin detalle: prudente
  assert.equal(c(30, { nSecc: 0 }), 'sinSecciones');
});

test('vigilada por el agente: 6 min y al frente solo si está apretada', () => {
  assert.equal(claseRelectura({ minAbierta: 8, vigilada: true }), 'vigilada');
  assert.equal(claseRelectura({ minAbierta: 40, hayLlena: true, vigilada: true }), 'vigilada'); // caza su NRC lleno
  assert.equal(claseRelectura({ minAbierta: 15, vigilada: true }), 'periodica');
  assert.equal(claseRelectura({ minAbierta: 45, vigilada: true }), 'holgada');  // 75 así el 23-nov
  assert.equal(claseRelectura({ minAbierta: 3, vigilada: true, nSecc: 0 }), 'sinSecciones');
  assert.equal(intervaloRelectura('vigilada', { sinCambios: 0, speed: 0.5 }), 0.025);
  assert.equal(intervaloRelectura('vigilada', { sinCambios: 9, speed: 0.5 }), 0.1);
});

test('intervalo por clase: base × 2 por lectura igual, con techo; más apretada = más seguido', () => {
  assert.equal(intervaloRelectura('critica', { sinCambios: 0, speed: 0.5 }), 0.05);   // 3 min
  assert.equal(intervaloRelectura('critica', { sinCambios: 9, speed: 0.5 }), 0.25);   // 15 min
  assert.equal(intervaloRelectura('activa', { sinCambios: 9, speed: 0.5 }), 0.5);     // 30 min
  assert.equal(intervaloRelectura('caza', { sinCambios: 9, speed: 0.5 }), 2);
  assert.equal(intervaloRelectura('periodica', { sinCambios: 0, speed: 0.5 }), 1.5);
  assert.equal(intervaloRelectura('periodica', { sinCambios: 9, speed: 0.5 }), 4);
  assert.equal(intervaloRelectura('holgada', { sinCambios: 0, speed: 0.5 }), 12);
  assert.equal(intervaloRelectura('holgada', { sinCambios: 9, speed: 0.5 }), 24);
  assert.equal(intervaloRelectura('sinSecciones', { sinCambios: 0, speed: 0.5 }), 6);
  assert.equal(intervaloRelectura('sinSecciones', { sinCambios: 9, speed: 0.5 }), 12);
  assert.equal(intervaloRelectura('desconocida', { sinCambios: 9 }), 24);
  assert.ok(intervaloRelectura('critica', { speed: 0 }) > 0);
  const orden = ['vigilada', 'critica', 'activa', 'caza', 'periodica', 'holgada'];
  for (const k of ['agotada', 'nuevaSeccion']) assert.equal(intervaloRelectura(k, { sinCambios: 9, speed: 0.5 }), 0.5);
  for (const n of [0, 2, 9]) {
    for (let i = 1; i < orden.length; i++) {
      assert.ok(intervaloRelectura(orden[i], { sinCambios: n, speed: 0.5 }) >= intervaloRelectura(orden[i - 1], { sinCambios: n, speed: 0.5 }), `${orden[i]} (${n})`);
    }
  }
  for (const [k, v] of Object.entries(CLASES)) assert.ok(v.base > 0 && v.techo >= v.base * 0.5, k);
});

test('resumen de secciones desde la memoria de la última lectura', () => {
  assert.equal(resumenSecciones(null), null);
  assert.equal(resumenSecciones({ t: 1 }), null);
  assert.deepEqual(resumenSecciones({ d: { 1: 25, 2: 7, 3: 40 }, m: 7 }), { minAbierta: 7, hayLlena: false });
  assert.deepEqual(resumenSecciones({ d: { 1: 0, 2: 30 }, m: 0 }), { minAbierta: 30, hayLlena: true });
  // Sección llena que la UASD oculta: no está en d, pero m = 0 lo delata.
  assert.deepEqual(resumenSecciones({ d: { 1: 22 }, m: 0 }), { minAbierta: 22, hayLlena: true });
  assert.deepEqual(resumenSecciones({ d: {}, m: 0 }), { minAbierta: Infinity, hayLlena: true });
});

test('secciones nuevas: la materia que estrena NRC queda en vigilancia activa 6 h', () => {
  // compararLectura cuenta los NRC que no estaban en la lectura anterior.
  const fila = (nrc, disponibles) => ({ nrc, disponibles, periodo: 'P' });
  assert.equal(compararLectura(null, [fila('1', 20)]).nuevos, 0);            // primera vez: no es «nueva»
  assert.equal(compararLectura({ 1: 20 }, [fila('1', 20)]).nuevos, 0);
  const r = compararLectura({ 1: 20 }, [fila('1', 20), fila('2', 30)]);
  assert.equal(r.nuevos, 1);
  assert.equal(r.cambio, true);
  assert.equal(compararLectura({}, [fila('7', 40)]).nuevos, 1);              // abrió la primera sección
  assert.equal(compararLectura({ 1: 20, 2: 5 }, [fila('1', 20), fila('2', 4)]).nuevos, 0);
  // Con > 20 cupos en todas sería holgada (24 h); tras estrenar sección, activa (≤ 30 min).
  assert.equal(claseRelectura({ minAbierta: 30, recienNueva: true }), 'nuevaSeccion');
  assert.equal(claseRelectura({ minAbierta: 30, hayLlena: true, recienNueva: true }), 'nuevaSeccion');
  assert.equal(claseRelectura({ minAbierta: 3, recienNueva: true }), 'critica');   // lo más apretado manda
  assert.equal(claseRelectura({ minAbierta: 30, recienNueva: false }), 'holgada');
  assert.equal(NUEVA_SECCION_MS, 6 * 3_600_000);
});
