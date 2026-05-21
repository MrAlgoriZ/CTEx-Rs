# **ROOT**

## `GET /`

**Описание:** Возвращает структуру API.

```bash
curl -X GET http://localhost:PORT/
```

# **HEALTH CHECK**

## `GET /health`

```bash
curl -X GET http://localhost:PORT/health
```

# **CYCLES**

## **1. Список активных циклов**

### `GET /cycles`

```bash
curl -X GET http://localhost:PORT/cycles
```

## **2. Запуск цикла (cycle_add)**

### `POST /cycles`

Тело запроса:

```json
{
  "symbol": "BTCUSDT",
  "type": "training",
  "password": "secret"
}
```

Пример вызова:

```bash
curl -X POST http://localhost:PORT/cycles \
  -H "Content-Type: application/json" \
  -d '{
        "symbol": "BTCUSDT",
        "type": "training",
        "password": "secret"
      }'
```

## **3. Остановка одного цикла (cycle_stop)**

### `DELETE /cycles/{symbol}`

Тело запроса:

```json
{
  "password": "secret"
}
```

Пример:

```bash
curl -X DELETE http://localhost:PORT/cycles/BTCUSDT \
  -H "Content-Type: application/json" \
  -d '{ "password": "secret" }'
```

## **4. Остановка всех циклов (cycles_stop_all)**

### `DELETE /cycles`

Тело запроса:

```json
{
  "password": "secret"
}
```

Пример:

```bash
curl -X DELETE http://localhost:PORT/cycles \
  -H "Content-Type: application/json" \
  -d '{ "password": "secret" }'
```

---

# **ACCURACY**

Все методы accuracy поддерживают **необязательный параметр `?window=N`**.
Если `window` не указан — используется дефолт из конфига.

## **1. Total accuracy**

### `GET /accuracy/total`

```bash
curl -X GET http://localhost:PORT/accuracy/total
```

Или:

```bash
curl -X GET "http://localhost:PORT/accuracy/total?window=100"
```

## **2. Accuracy одного токена**

### `GET /accuracy/{symbol}`

```bash
curl -X GET http://localhost:PORT/accuracy/BTCUSDT
```

Или с window:

```bash
curl -X GET "http://localhost:PORT/accuracy/BTCUSDT?window=100"
```

## **3. Accuracy всех активных токенов**

### `GET /accuracy`

```bash
curl -X GET http://localhost:PORT/accuracy
```

или:

```bash
curl -X GET "http://localhost:PORT/accuracy?window=100"
```

## **4. Сохраение на сервер графика точности**

### `POST /plot/{symbol}`

```bash
curl -X POST http://localhost:PORT/plot/BTCUSDT \
  -H "Content-Type: application/json" \
  -d '{ "password": "secret" }'
```
