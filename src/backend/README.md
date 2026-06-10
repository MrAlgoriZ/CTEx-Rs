# **ROOT**

## `GET /`

**Description:** Returns the API structure

```bash
curl -X GET http://localhost:PORT/
```

# **HEALTH CHECK**

## `GET /health`

```bash
curl -X GET http://localhost:PORT/health
```

# **CYCLES**

## **1. Active cycles list**

### `GET /cycles`

```bash
curl -X GET http://localhost:PORT/cycles
```

## **2. Cycle start (cycle_add)**

### `POST /cycles`

Request body:

```json
{
  "symbol": "BTCUSDT",
  "type": "training",
  "password": "secret"
}
```

Request example:

```bash
curl -X POST http://localhost:PORT/cycles \
  -H "Content-Type: application/json" \
  -d '{
        "symbol": "BTCUSDT",
        "type": "training",
        "password": "secret"
      }'
```

## **3. Stop single cycle (cycle_stop)**

### `DELETE /cycles/{symbol}`

Request body:

```json
{
  "password": "secret"
}
```

Request example:

```bash
curl -X DELETE http://localhost:PORT/cycles/BTCUSDT \
  -H "Content-Type: application/json" \
  -d '{ "password": "secret" }'
```

## **4. Stop all cycles (cycles_stop_all)**

### `DELETE /cycles`

Request body:

```json
{
  "password": "secret"
}
```

Request example:

```bash
curl -X DELETE http://localhost:PORT/cycles \
  -H "Content-Type: application/json" \
  -d '{ "password": "secret" }'
```

---

# **ACCURACY**

All accuracy methods support the optional parameter `?window=N`.
If `window` is not specified, the default from the config is used.

## **1. Total accuracy**

### `GET /accuracy/total`

```bash
curl -X GET http://localhost:PORT/accuracy/total
```

or:

```bash
curl -X GET "http://localhost:PORT/accuracy/total?window=100"
```

## **2. Accuracy of single token**

### `GET /accuracy/{symbol}`

```bash
curl -X GET http://localhost:PORT/accuracy/BTCUSDT
```

or:

```bash
curl -X GET "http://localhost:PORT/accuracy/BTCUSDT?window=100"
```

## **3. Accuracy of all active tokens**

### `GET /accuracy`

```bash
curl -X GET http://localhost:PORT/accuracy
```

or:

```bash
curl -X GET "http://localhost:PORT/accuracy?window=100"
```

---

# **PLOTS**

Save plots with accuracy to server

### `POST /plot/{symbol}`

```bash
curl -X POST http://localhost:PORT/plot/BTCUSDT \
  -H "Content-Type: application/json" \
  -d '{ "password": "secret" }'
```
