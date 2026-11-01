# Distributed system

A subject done by University of Alicante.

## File structure

- p0 (by n4th0)
- p1 (by n4th0 and mcp173)

## Practice


monitor

docker compose run -d --rm --name wm-monitor-1 -e WM_WS_ID=WS-01 -e WM_WS_LOCATION="River Park" -e WM_WS_SECRET=s3cret-1 monitor

engine

docker compose run --rm --name wm-engine-1 -e WM_MONITOR=wm-monitor-1:7001 engine



FO

docker compose -f docker-compose.lab-fo.yml --env-file .env build  

docker compose -f docker-compose.lab-fo.yml --env-file .env run --rm fo1



## Colaborators

n4th0 -> nrm69@alu.ua.es
mcp173 -> mcp173@alu.ua.es
