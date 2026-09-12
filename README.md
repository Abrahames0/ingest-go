# ingest-go — ingestión de notificaciones bancarias

Microservicio mínimo en Go (biblioteca estándar + `go-redis`) que recibe las
notificaciones que la app captura en el teléfono, las normaliza, descarta las
repetidas y las encola en un Redis Stream. La API de NestJS (`api-fin`) las
consume desde ahí (`CaptureStreamConsumer`), las lee con reglas o con el
modelo chico y las deja en la bandeja de movimientos por confirmar
(`GET /api/v1/capture`).

Por qué existe: el teléfono dispara y olvida. Aunque la API esté reiniciando o
el modelo tarde, la notificación ya quedó a salvo en la cola.

```
teléfono ──POST /ingest/notification──▶ ingest-go ──XADD──▶ Redis Stream ──XREADGROUP──▶ api-fin ──▶ bandeja
```

## Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/healthz` | `200 {status:"ok", version, commit}` si Redis responde, `503 {status:"degraded"}` si no |
| `POST` | `/ingest/notification` | Una notificación. `202 {queued:true,id}` o `200 {queued:false,duplicate:true}` |
| `POST` | `/ingest/notifications` | Hasta 50 en `{items:[...]}` (sincronización offline). `200 {received,queued,duplicates,rejected,results}` |

Autenticación en los dos `POST`: header `x-api-key` (el `API_KEY` de la API) y
`Authorization: Bearer <token>`, que puede ser el token de acceso que emite
`POST /api/v1/auth/login` (15 min) o un token de captura de larga vida de
`POST /api/v1/capture/tokens` (scope `capture`, un año). Los dos se verifican
aquí con el mismo `JWT_ACCESS_SECRET`, sin llamar a la API. Los tokens de
captura se pueden revocar: la API escribe el `jti` en el set de Redis
`capture:revoked` y este servicio lo consulta en cada petición. Un token de
acceso cerrado con `logout` sigue valiendo aquí hasta que expira (a lo más 15
min): es por diseño, para no depender de la API en cada notificación.

Antes de autenticar hay un freno por dirección IP (`IP_RATE_PER_MINUTE`, 600 por
defecto, primer salto de `X-Forwarded-For` detrás de Traefik) contra la fuerza
bruta sobre la API key.

Cuerpo de una notificación (mismo contrato que `POST /api/v1/capture/notifications`):

```json
{
  "text": "Compra por $350.00 en OXXO con tu tarjeta terminación 1234 el 10/09/2026 14:32",
  "title": "BBVA",
  "packageName": "com.bancomer.mbanking",
  "appName": "BBVA México",
  "postedAt": "2026-09-10T20:32:10.000Z",
  "deviceId": "pixel-8",
  "source": "NOTIFICATION"
}
```

Reglas: `text` obligatorio (≤ 2000 caracteres), `postedAt` en RFC 3339 (si falta,
ahora; ni más de 24 h en el futuro ni más de 90 días en el pasado), `source`
`NOTIFICATION` o `SMS`. Cuerpo de a lo más 256 KB (`413`), solo `application/json`
(`415`). Límite de 120 por minuto por usuario (`429`). La misma notificación del
mismo usuario el mismo día (hora de Ciudad de México) se responde como duplicada
sin encolarla; la API vuelve a deduplicar con su propia huella. Un lote se procesa
completo aunque el teléfono corte la conexión a la mitad.

## Contrato del stream

`XADD capture:notifications MAXLEN ~ 50000 *` con campos (todos string):
`userId`, `source`, `packageName`, `appName`, `title`, `text`, `postedAt`,
`receivedAt`, `deviceId`. La API los lee con el grupo de consumidores `api`
(`CAPTURE_STREAM_KEY` / `CAPTURE_CONSUMER_GROUP` en `api-fin/.env`) y confirma
cada entrada al guardarla; las que fallan tres veces van a `capture:notifications:dead`.

## Configuración

Variables de entorno (o un `.env` junto al binario; ver `.env.example`):

| Variable | Default | Nota |
|---|---|---|
| `API_KEY` | — | Igual que en `api-fin` |
| `JWT_ACCESS_SECRET` | — | Igual que en `api-fin` |
| `REDIS_URL` | `redis://localhost:6379` | El mismo Redis que usa la API |
| `PORT` | `8080` | |
| `CAPTURE_STREAM_KEY` | `capture:notifications` | Igual que en `api-fin` |
| `CAPTURE_STREAM_MAXLEN` | `50000` | Tope aproximado del stream |
| `MAX_TEXT_LENGTH` | `2000` | |
| `RATE_PER_MINUTE` | `120` | Por usuario, después de autenticar |
| `IP_RATE_PER_MINUTE` | `600` | Por dirección IP, antes de autenticar |

## Correr

```bash
cp .env.example .env   # y pega API_KEY, JWT_ACCESS_SECRET y REDIS_URL de api-fin/.env
go run .
go test ./...
go vet ./...
```

Las pruebas del paquete `queue` corren contra un Redis real cuando `REDIS_URL`
está definido (en CI hay uno); si no, se saltan.

Docker (Dokploy lo construye igual desde este `Dockerfile`):

```bash
docker build -t centli-ingest --build-arg VERSION=1.0.0 --build-arg COMMIT=$(git rev-parse --short HEAD) .
docker run --rm --env-file .env -p 8080:8080 centli-ingest
```

La imagen trae `HEALTHCHECK`: el propio binario se sondea con `ingest -check`
(la imagen distroless no tiene `curl`). `/healthz` muestra la versión y el
commit con que se construyó.

## Probar a mano

Sin levantar la API, `go run ./cmd/token <userId> [ttl]` firma un token con el
mismo secreto (la API sí exige que el usuario exista; este servicio no). En
Windows, `curl` re-codifica los argumentos a cp1252: manda el cuerpo con
`--data-binary @archivo.json` para que los acentos lleguen intactos.

```bash
TOKEN=$(curl -s -X POST http://localhost:3000/api/v1/auth/login \
  -H "x-api-key: $API_KEY" -H "content-type: application/json" \
  -d '{"email":"tu@correo.mx","password":"..."}' | jq -r .data.accessToken)

curl -i -X POST http://localhost:8080/ingest/notification \
  -H "x-api-key: $API_KEY" -H "Authorization: Bearer $TOKEN" \
  -H "content-type: application/json" \
  -d '{"text":"Compra por $350.00 en OXXO con tu tarjeta terminación 1234","title":"BBVA","packageName":"com.bancomer.mbanking"}'
```

Segundos después la captura aparece en `GET /api/v1/capture` de la API.
