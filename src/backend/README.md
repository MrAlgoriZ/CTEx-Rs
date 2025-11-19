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
  "symbol": "BTC",
  "type": "training",
  "password": "secret"
}
```

Пример вызова:

```bash
curl -X POST http://localhost:PORT/cycles \
  -H "Content-Type: application/json" \
  -d '{
        "symbol": "BTC",
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
curl -X DELETE http://localhost:PORT/cycles/BTC \
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
curl -X GET http://localhost:PORT/accuracy/BTC
```

Или с window:

```bash
curl -X GET "http://localhost:PORT/accuracy/BTC?window=100"
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
