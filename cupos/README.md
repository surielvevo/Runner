# Cupos UASD · Programación docente de CupoPino

Actualiza la tabla `cupos_oficiales` de CupoPino (Supabase) leyendo la Programación
por asignatura de la UASD, desde **GitHub Actions** (`.github/workflows/cupos-uasd.yml`):
sin depender de ninguna computadora encendida.

| Archivo | Qué hace |
|---|---|
| `etl-cupos.mjs` | El ETL de siempre (Playwright + Supabase): cola por urgencia (menos cupos primero), corte ordenado por tiempo, latido en `etl_runs`, freno ante Cloudflare. Origen: `scripts/etl-cupos.mjs` de CupoUASD. |
| `proxies.mjs` | Pool de proxies públicos: varias fuentes, prefiltro de red en paralelo (¿abre túnel hasta `app.uasd.edu.do:443`?) y prueba con navegador real. Los que pasan se recuerdan entre turnos. |
| `diagnostico.mjs` | Comprueba, sin escribir nada, si la UASD responde desde el runner (directo y por proxies). |

## Puesta en marcha (sin claves)
Este repositorio público **no guarda ninguna clave**. Cada ejecución pide a GitHub un token de
identidad (OIDC, `permissions: id-token: write`) y la función `etl-puente` de Supabase verifica su
firma: solo acepta `surielvevo/Runner`, rama `main`, el workflow `cupos-uasd.yml`, disparado por
horario o a mano en un runner de GitHub, y solo deja tocar `cupos_oficiales`, `campus_catalogo`,
`etl_runs` y tres funciones del ETL. Escribe con la clave que vive dentro de Supabase.
Cada turno pide el siguiente al terminar (job `relevo`); el horario de cada 30 min queda de respaldo. Opcional: secreto `PROXY_SCRAPER_KEY`.

## Seguridad
- Ninguna clave en el repo ni en GitHub: el token OIDC dura minutos y solo vale para este workflow en `main`.
- Por un proxy solo viaja HTTPS de punta a punta (el navegador verifica el certificado). Supabase nunca va por proxy.
- El workflow solo se dispara por horario o a mano: un fork o un pull request no ejecuta nada con los secretos.

## Créditos de las listas de proxies
api.proxyscraper.com (datos CC BY 4.0, incluye direcciones de proxmint/free-proxy-list y proxio.io; geolocalización de DB-IP),
proxifly/free-proxy-list, TheSpeedX/PROXY-List, monosans/proxy-list y ProxyScrape.
