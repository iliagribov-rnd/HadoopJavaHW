# DZ1: Агрегация торговых сделок в свечи (OHLC Candles)

MapReduce задача для обработки потока торговых сделок и построения свечных графиков по временным интервалам.

## Задача

Преобразование детализированных данных торговых сделок (trade-by-trade) в агрегированные свечи (OHLC - Open, High, Low, Close) с фильтрацией по времени, датам и финансовым инструментам.

## Архитектура решения

### Mapper: CandleMapper

**Функции:**
- Парсинг CSV файлов с торговыми сделками
- Динамическое определение структуры CSV (чтение заголовков)
- Фильтрация по датам (`candle.date.from`, `candle.date.to`)
- Фильтрация по времени суток (`candle.time.from`, `candle.time.to`)
- Фильтрация по инструментам через regex (`candle.securities`)
- Распределение сделок по свечам заданной ширины

**Ключ вывода:** `SYMBOL|YYYYMMDD|CandleNum` (например, `SBER|20210115|042`)

**Значение вывода:** `TradeWritable(timeMs, idDeal, price)`

### Partitioner: TimePartitioner

Равномерное распределение свечей по reducers с учетом временной последовательности.

### Reducer: CandleAggregate

**Функции:**
- Вычисление OHLC по всем сделкам в свече
- Open: первая сделка (по времени и ID)
- High: максимальная цена
- Low: минимальная цена
- Close: последняя сделка (по времени и ID)

**Вывод:** MultipleOutputs с разделением по инструментам

## Формат данных

### Входной CSV

```
#SYMBOL,TYPE_DEAL,MOMENT,ID_DEAL,PRICE_DEAL,QUANTITY,VOLUME_RUB,OI
SBER,1,201501151000123,1234567,95.50,100,9550.0,0
SBER,1,201501151001456,1234568,95.55,50,4777.5,0
GAZP,1,201501151002789,1234569,180.20,200,36040.0,0
```

**Поля:**
- `#SYMBOL` - тикер инструмента
- `MOMENT` - дата и время (YYYYMMDDHHMMSSmmm)
- `ID_DEAL` - уникальный ID сделки
- `PRICE_DEAL` - цена сделки
- `QUANTITY` - объем
- `VOLUME_RUB` - объем в рублях
- `OI` - открытый интерес

### Выходной формат

```
SYMBOL,MOMENT,OPEN,HIGH,LOW,CLOSE
SBER,201501151000000,95.5,95.8,95.4,95.7
SBER,201501151005000,95.7,96.2,95.6,96.0
```

Файлы создаются отдельно для каждого инструмента: `SBER-r-00000`, `GAZP-r-00000`, и т.д.

## Конфигурация

### Параметры задачи

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `candle.width` | Ширина свечи в миллисекундах | 300000 (5 мин) |
| `candle.date.from` | Начальная дата (YYYYMMDD) | 19000101 |
| `candle.date.to` | Конечная дата (YYYYMMDD) | 20200101 |
| `candle.time.from` | Начало торговой сессии (HHMM) | 1000 |
| `candle.time.to` | Конец торговой сессии (HHMM) | 1800 |
| `candle.securities` | Regex для фильтрации инструментов | `.*` (все) |
| `candle.num.reducers` | Количество reducer'ов | 1 |

### Пример config.xml

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>candle.width</name>
    <value>300000</value>
  </property>
  <property>
    <name>candle.time.from</name>
    <value>1000</value>
  </property>
  <property>
    <name>candle.time.to</name>
    <value>1800</value>
  </property>
  <property>
    <name>candle.securities</name>
    <value>SBER|GAZP|LKOH</value>
  </property>
  <property>
    <name>candle.num.reducers</name>
    <value>4</value>
  </property>
</configuration>
```

## Сборка

### Maven

```bash
cd candle
mvn clean package
# Результат: target/candle.jar
```

### Docker

```bash
cd dz1
docker buildx build --platform linux/amd64 -t candle:latest --load .
```

**Dockerfile:**
- Base: Ubuntu 22.04
- Java 8 (OpenJDK)
- Hadoop 3.3.6
- Maven сборка при создании образа

## Запуск

### Локальный Hadoop

```bash
# Базовый запуск
hadoop jar target/candle.jar input.csv output/

# С параметрами через -D флаги
hadoop jar target/candle.jar \
  -D candle.width=600000 \
  -D candle.time.from=1000 \
  -D candle.time.to=1800 \
  -D candle.securities="SBER|GAZP" \
  -D candle.num.reducers=4 \
  input/ output/

# С конфигурационным файлом
hadoop jar target/candle.jar -conf config.xml input/ output/
```

### Docker

```bash
# Запуск контейнера
docker run --rm -it --entrypoint bash \
  -v $(pwd)/data:/candle/data \
  candle:latest

# Внутри контейнера
/opt/hadoop-3.3.6/bin/hadoop jar target/candle.jar \
  -conf /candle/data/config.xml \
  /candle/data/input.csv \
  /candle/data/output
```

## Структура проекта

```
candle/
├── src/main/java/
│   └── Candle.java           # Основной класс
│       ├── TradeWritable     # Writable для сделки
│       ├── CandleMapper      # Mapper класс
│       ├── TimePartitioner   # Custom Partitioner
│       └── CandleAggregate   # Reducer класс
└── pom.xml                   # Maven конфигурация
```

## Примеры использования

### Свечи 1 минута для всей сессии

```bash
hadoop jar target/candle.jar \
  -D candle.width=60000 \
  -D candle.time.from=1000 \
  -D candle.time.to=1800 \
  input/ output/
```

### Только голубые фишки, 5-минутные свечи

```bash
hadoop jar target/candle.jar \
  -D candle.width=300000 \
  -D candle.securities="SBER|GAZP|LKOH|GMKN|NVTK" \
  input/ output/
```

### Обработка конкретного дня

```bash
hadoop jar target/candle.jar \
  -D candle.date.from=20210115 \
  -D candle.date.to=20210116 \
  input/ output/
```

## Зависимости

```xml
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-common</artifactId>
  <version>3.3.6</version>
</dependency>
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-mapreduce-client-core</artifactId>
  <version>3.3.6</version>
</dependency>
```
