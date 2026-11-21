// Pruebas de ritmo.mjs (sin red). Correr: npm test (desde cupos/).
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  NIVELES, NIVEL_LENTO, ritmoInicial, ajustarRitmo, techoHoras, intervaloHoras,
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

test('intervalo: lo que no cambia se relee cada vez menos, con techo por tipo', () => {
  const critica = { base: 0.025, minDisp: 2 };          // 1,5 min
  assert.equal(intervaloHoras({ ...critica, sinCambios: 0 }), 0.025);
  assert.equal(intervaloHoras({ ...critica, sinCambios: 1 }), 0.05);
  assert.equal(intervaloHoras({ ...critica, sinCambios: 4 }), 0.4);
  assert.equal(intervaloHoras({ ...critica, sinCambios: 50 }), 0.4); // ×16 máximo
  const llena = { base: 0.05, minDisp: 0 };              // 3 min → techo 30 min
  assert.equal(intervaloHoras({ ...llena, sinCambios: 9 }), 0.5);
  const holgada = { base: 4, minDisp: 30 };              // 4 h → techo 8 h
  assert.equal(intervaloHoras({ ...holgada, sinCambios: 0 }), 4);
  assert.equal(intervaloHoras({ ...holgada, sinCambios: 3 }), 8);
  const sinSecciones = { base: 4, minDisp: 999, nSecc: 0 };
  assert.equal(intervaloHoras({ ...sinSecciones, sinCambios: 9 }), 24);
});

test('intervalo de una vigilada por el agente: base corta y techo de 6 min', () => {
  assert.equal(intervaloHoras({ base: 0.025, minDisp: 0, fija: true, sinCambios: 0 }), 0.025);
  assert.equal(intervaloHoras({ base: 0.025, minDisp: 0, fija: true, sinCambios: 9 }), 0.1);
  assert.equal(techoHoras({ minDisp: 50, fija: true }), 0.1);
});

test('intervalo nunca baja de la base ni pasa el techo aunque la base sea rara', () => {
  assert.equal(intervaloHoras({ base: 16, minDisp: 30, sinCambios: 0 }), 8);
  assert.ok(intervaloHoras({ base: 0, minDisp: 2, sinCambios: 0 }) > 0);
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
