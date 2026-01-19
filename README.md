# Hadoop MapReduce

Репозиторий содержит два решения задач по распределенной обработке данных с использованием Hadoop MapReduce.

## Структура проекта

```
HadoopJavaHW/
├── dz1/                # Агрегация торговых сделок в свечи (OHLC)
│   ├── candle/         # Maven проект
│   │   ├── src/main/java/Candle.java
│   │   └── pom.xml
│   ├── Dockerfile
│   └── README.md
└── dz2/                # Перемножение больших разреженных матриц
    ├── mm/             # Maven проект
    │   ├── src/main/java/mm.java
    │   └── pom.xml
    ├── Dockerfile
    └── README.md
```

## Задачи

### DZ1: Агрегация торговых сделок в свечи

MapReduce задача для обработки потока торговых сделок и построения свечных графиков (OHLC - Open, High, Low, Close) по заданным временным интервалам.

**Функционал:**
- Фильтрация сделок по датам, времени и инструментам
- Группировка сделок в свечи заданной ширины
- Вычисление OHLC для каждой свечи
- Custom Partitioner для оптимального распределения
- MultipleOutputs для вывода по инструментам

**Технологии:** Hadoop 3.3.6, Java 8, Maven

[Подробнее →](dz1/README.md)

### DZ2: Умножение больших разреженных матриц

MapReduce задача для распределенного умножения больших разреженных матриц с использованием блочного алгоритма и двух MapReduce проходов.

**Функционал:**
- Block-based умножение матриц (A × B = C)
- Двухпроходный алгоритм MapReduce
- Поддержка разреженных матриц (пропуск нулей)
- SequenceFile для эффективной промежуточной передачи
- Статистика производительности (shuffle bytes, records)

**Технологии:** Hadoop 3.3.6, Java 8, Maven

[Подробнее →](dz2/README.md)

## Требования

- Java 8
- Hadoop 3.3.6
- Maven 3.x
- Docker (опционально для сборки)

## Быстрый старт

### Локальная сборка

```bash
# DZ1
cd dz1/candle
mvn clean package

# DZ2
cd dz2/mm
mvn clean package
```

### Docker сборка

```bash
# DZ1
cd dz1
docker buildx build --platform linux/amd64 -t candle:latest --load .

# DZ2
cd dz2
docker buildx build --platform linux/amd64 -t mm:latest --load .
```

## Запуск

### DZ1 - Свечи

```bash
hadoop jar dz1/candle/target/candle.jar \
  -D candle.width=300000 \
  -D candle.time.from=1000 \
  -D candle.time.to=1800 \
  input/ output/
```

### DZ2 - Матрицы

```bash
hadoop jar dz2/mm/target/mm.jar mm \
  -D mm.groups=4 \
  -D mapred.reduce.tasks=8 \
  A/ B/ C/
```

## Тестирование

Каждая задача содержит тестовые данные и config.xml для проверки на локальном кластере или в Docker.
