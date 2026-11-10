# Cupos UASD · Programación docente de CupoPino

Actualiza la tabla `cupos_oficiales` de CupoPino (Supabase) leyendo la Programación
por asignatura de la UASD, desde **GitHub Actions** (`.github/workflows/cupos-uasd.yml`):
sin depender de ninguna computadora encendida.

| Archivo | Qué hace |
|---|---|
| `etl-cupos.mjs` | El ETL de siempre (Playwright + Supabase): cola por urgencia (menos cupos primero), corte ordenado por tiempo, latido en `etl_runs`, freno ante Cloudflare. Origen: `scripts/etl-cupos.mjs` de CupoUASD. |
| `proxies.mjs` | Pool de proxies públicos: varias fuentes, prefiltro de red en paralelo (¿abre túnel hasta `app.uasd.edu.do:443`?) y prueba con navegador real. Los que pasan se recuerdan entre turnos. |
| `diagnostico.mjs` | Comprueba, sin escribir nada, si la UASD responde desde el runner (directo y por proxies). |

## Puesta en marcha
1. **Settings → Secrets and variables → Actions → New repository secret**:
   `NEXT_PUBLIC_SUPABASE_URL` y `SUPABASE_SERVICE_ROLE_KEY` (los mismos de Vercel).
   Opcional: `PROXY_SCRAPER_KEY` (clave gratis de api.proxyscraper.com, más consultas por minuto).
2. **Actions → Cupos UASD → Run workflow** con `modo = diagnostico` para ver si Cloudflare deja pasar.
3. Con los secretos puestos, el horario arranca solo cada 30 min y encadena turnos.

## Seguridad
- Los secretos solo viven en GitHub Secrets; nunca en el código ni en los registros.
- Por un proxy solo viaja HTTPS de punta a punta (el navegador verifica el certificado). Supabase nunca va por proxy.
- El workflow solo se dispara por horario o a mano: un fork o un pull request no ejecuta nada con los secretos.

## Créditos de las listas de proxies
api.proxyscraper.com (datos CC BY 4.0, incluye direcciones de proxmint/free-proxy-list y proxio.io; geolocalización de DB-IP),
proxifly/free-proxy-list, TheSpeedX/PROXY-List, monosans/proxy-list y ProxyScrape.
