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
| `GET` | `/healthz` | `200 {status:"ok", version, commit}` si Redis responde, `503 {status:"degraded"}` si no. El `HEALTHCHECK` del contenedor (`ingest -check`) acepta ambos: un Redis caído no reinicia el servicio en bucle |
| `POST` | `/ingest/notification` | Una notificación. `202 {queued:true,id}` o `200 {queued:false,duplicate:true}` |
| `POST` | `/ingest/notifications` | Hasta 50 en `{items:[...]}` (sincronización offline). `200 {received,queued,duplicates,rejected,retry,results}`; cada `results[i]` trae `status` (202 encolada, 200 repetida, 400 inválida, 429 fuera de límite, 503 cola caída). `rejected` se descarta, `retry` se reenvía; si la cola falló con algún elemento la respuesta completa es `503` para que el teléfono reenvíe el lote (las repetidas se filtran solas) |

## Autenticación

Los dos `POST` piden el header `x-api-key` (el `API_KEY` de la API) y
`Authorization: Bearer <token de captura>`. Solo se aceptan tokens de captura,
los de larga vida que emite `POST /api/v1/capture/tokens` para el atajo de iOS
y el listener de Android: JWT HS256 firmados con `CAPTURE_TOKEN_SECRET`, un
secreto propio que no firma nada más. Este servicio exige `iss: centli`,
`aud: centli-capture`, `scope: capture`, `sub` (el usuario), `jti` (el id del
token) y un `exp` vigente, y rechaza cualquier otro algoritmo. Un token de
acceso de la API (`aud: centli-api`, firmado con otro secreto) recibe `401`.

La firma no basta. La API lleva una lista de tokens activos en Redis:
`capture:active:{jti}` guarda el id del usuario y caduca junto con el token.
Revocar un token, revocarlos todos o desactivar la cuenta borra la entrada, y en
cada petición este servicio exige que exista y coincida con `sub`. Sin la
entrada, aunque Redis haya perdido sus datos, la respuesta es `401`: el servicio
falla cerrado.

Antes de autenticar hay un freno por dirección IP (`IP_RATE_PER_MINUTE`, 600 por
defecto) contra la fuerza bruta sobre la API key. La dirección sale de
`X-Forwarded-For` según `TRUST_PROXY_HOPS`, el número de proxies delante del
servicio: con N se toma la entrada N lugares desde la derecha, contando la
dirección del socket como la última, igual que `trust proxy` de Express en la
API. Las entradas más a la izquierda las escribe el cliente y se ignoran. `0` en
local, `1` detrás de Traefik y `2` si además pasa por el proxy de Cloudflare.

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
(`CAPTURE_STREAM_KEY` / `CAPTURE_CONSUMER_GROUP` en `api-fin/.env`), confirma y
borra cada entrada al guardarla, así que el texto no se queda en Redis. Las que
fallan tres veces se anotan en `capture:notifications:dead` sin `text` ni `title`,
y cada noche la API recorta del stream lo que tenga más de 7 días.

## Configuración

Variables de entorno (o un `.env` junto al binario; ver `.env.example`):

| Variable | Default | Nota |
|---|---|---|
| `API_KEY` | — | Igual que en `api-fin` |
| `CAPTURE_TOKEN_SECRET` | — | Igual que en `api-fin`; 32+ caracteres y distinto de `API_KEY` |
| `REDIS_URL` | `redis://localhost:6379` | El Redis de la API; en producción, con el usuario ACL de abajo |
| `TRUST_PROXY_HOPS` | `0` | Proxies delante del servicio, de 0 a 5 |
| `PORT` | `8080` | |
| `CAPTURE_STREAM_KEY` | `capture:notifications` | Igual que en `api-fin` |
| `CAPTURE_STREAM_MAXLEN` | `50000` | Tope aproximado del stream |
| `MAX_TEXT_LENGTH` | `2000` | |
| `RATE_PER_MINUTE` | `120` | Por usuario, después de autenticar |
| `IP_RATE_PER_MINUTE` | `600` | Por dirección IP, antes de autenticar |

## Usuario ACL de Redis para producción

Con la contraseña de Redis de la API, este servicio podría leer los tokens de
actualización (`rt:*`) y los enlaces para cambiar la contraseña (`pr:*`). En
producción dale un usuario propio (Redis 7 o posterior), limitado a sus llaves,
a lo que hace con cada una y a los comandos que ejecuta:

```
ACL SETUSER ingest on >CONTRASEÑA_LARGA resetkeys %RW~ingest:* %R~capture:active:* %RW~capture:notifications resetchannels -@all +ping +get +set +incr +expire +eval +evalsha +xadd +client|setinfo
```

- `%RW~ingest:*`: los contadores de los frenos (`INCR` y `EXPIRE` dentro de un script con `EVALSHA`, o `EVAL` la primera vez) y el filtro de repetidas (`SET NX`).
- `%R~capture:active:*`: la lista de tokens de captura activos, solo lectura. Aunque alguien tomara el servicio, no podría dar de alta tokens.
- `%RW~capture:notifications`: el stream. `XADD` con `MAXLEN ~` cuenta como lectura y escritura para las ACL de Redis 7 (recorta al insertar), así que `%W` solo daría `NOPERM`; el usuario no tiene `XRANGE` ni `XREAD`, con lo que sigue sin poder leer las notificaciones de nadie. El patrón es exacto: la cola muerta `capture:notifications:dead` es de la API.
- `PING` responde `/healthz`. `AUTH` y `HELLO` no necesitan permiso; `client|setinfo` es el saludo con que go-redis anuncia su versión (sin él todo funciona, pero cada conexión deja dos rechazos en `ACL LOG`).

Luego `REDIS_URL=redis://ingest:CONTRASEÑA_LARGA@<host>:6379/0`. Con una base
distinta de la `0`, go-redis manda `SELECT` y hay que agregar `+select`. Si
cambias `CAPTURE_STREAM_KEY`, cambia también ese patrón. `ACL SETUSER` no
sobrevive un reinicio si Redis no usa `aclfile`: pon la regla en el archivo ACL o
como `user ingest on ...` en la configuración. Para revisar un permiso sin
ejecutarlo: `ACL DRYRUN ingest XADD capture:notifications MAXLEN ~ 50000 * userId x`.

## Correr

```bash
cp .env.example .env   # y pega API_KEY, CAPTURE_TOKEN_SECRET y REDIS_URL de api-fin/.env
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

Con la API corriendo, pide un token de captura con un token de acceso y úsalo
aquí. En Windows, `curl` re-codifica los argumentos a cp1252: manda el cuerpo
con `--data-binary @archivo.json` para que los acentos lleguen intactos.

```bash
ACCESS=$(curl -s -X POST http://localhost:3000/api/v1/auth/login \
  -H "x-api-key: $API_KEY" -H "content-type: application/json" \
  -d '{"email":"tu@correo.mx","password":"..."}' | jq -r .data.accessToken)

TOKEN=$(curl -s -X POST http://localhost:3000/api/v1/capture/tokens \
  -H "x-api-key: $API_KEY" -H "Authorization: Bearer $ACCESS" \
  -H "content-type: application/json" -d '{"name":"curl"}' | jq -r .data.token)

curl -i -X POST http://localhost:8080/ingest/notification \
  -H "x-api-key: $API_KEY" -H "Authorization: Bearer $TOKEN" \
  -H "content-type: application/json" \
  -d '{"text":"Compra por $350.00 en OXXO con tu tarjeta terminación 1234","title":"BBVA","packageName":"com.bancomer.mbanking"}'
```

Segundos después la captura aparece en `GET /api/v1/capture` de la API.

Sin la API, `go run ./cmd/token -sub <userId> -activate` firma un token con el
mismo secreto y lo da de alta en la lista de activos de `REDIS_URL`. Es solo para
desarrollo: la API exige que el usuario exista y esta herramienta no.
