# DZ2: Умножение больших разреженных матриц (Matrix Multiplication)

MapReduce задача для распределенного умножения больших разреженных матриц с использованием блочного алгоритма.

## Задача

Вычисление произведения C = A × B для больших разреженных матриц, где:
- A: размер M × N
- B: размер N × K  
- C: размер M × K (результат)

Алгоритм использует блочное разбиение и два прохода MapReduce для эффективной обработки матриц, не помещающихся в память.

## Архитектура решения

### Двухпроходный алгоритм

#### Проход 1: Группировка блоков (MatrixBlockMapper + MatrixBlockAggregate)

**MatrixBlockMapper:**
- Читает элементы матриц A и B из входных файлов
- Разбивает матрицы на блоки размера `group × group`
- Для A[i,k]: генерирует ключи для всех блоков (i, *, k)
- Для B[k,j]: генерирует ключи для всех блоков (*, j, k)
- **Ключ:** `i|j|k` (номера блоков)
- **Значение:** `BlockWritable(tag, local_i, local_j, value)`

**MatrixBlockAggregate:**
- Группирует элементы A и B по блокам (i,j,k)
- Выполняет локальное умножение блоков: C_ij += A_ik × B_kj
- Сохраняет частичные произведения в SequenceFile

#### Проход 2: Суммирование блоков (MatrixSumMapper + MatrixSumAggregate)

**MatrixSumMapper:**
- Читает частичные произведения из SequenceFile
- Перегруппировывает по финальным позициям (i,j)

**MatrixSumAggregate:**
- Суммирует частичные произведения по всем k
- Вычисляет итоговые элементы C[i,j]
- Пропускает нулевые элементы (sparse optimization)

## Формат данных

### Входные матрицы

Каждая матрица хранится в двух файлах:

**data** - элементы матрицы (TSV):
```
A	0	0	1.5
A	0	1	2.3
A	1	0	0.8
B	0	0	3.2
B	0	1	1.1
B	1	0	2.5
```

Формат: `TAG\tROW\tCOL\tVALUE`

**size** - размеры матрицы (TSV):
```
800	600
```

Формат: `HEIGHT\tWIDTH`

### Структура директорий

```
A/
├── data     # Элементы матрицы A
└── size     # Размеры: M×N

B/
├── data     # Элементы матрицы B  
└── size     # Размеры: N×K

C/
├── data/    # Результат C (создается)
└── size     # Размеры: M×K (создается)
```

### Выходная матрица

```
C	0	0	8.95
C	0	1	7.40
C	1	0	4.56
```

Формат соответствует входному.

## Конфигурация

### Параметры задачи

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `mm.groups` | Количество блоков по каждой размерности | 1 |
| `mm.tags` | Теги матриц (A, B, C) | "ABC" |
| `mm.float-format` | Формат чисел в выводе | "%.3f" |
| `mapred.reduce.tasks` | Количество reducer'ов | 1 |

### Автоматически вычисляемые

- `mm.fst.size.h` - высота матрицы A (из файла size)
- `mm.fst.size.w` - ширина матрицы A
- `mm.snd.size.h` - высота матрицы B
- `mm.snd.size.w` - ширина матрицы B

### Пример config.xml

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mm.groups</name>
    <value>4</value>
  </property>
  <property>
    <name>mapred.reduce.tasks</name>
    <value>8</value>
  </property>
  <property>
    <name>mm.float-format</name>
    <value>%.3f</value>
  </property>
</configuration>
```

## Сборка

### Maven

```bash
cd mm
mvn clean package
# Результат: target/mm.jar
```

### Docker

```bash
cd dz2
docker buildx build --platform linux/amd64 -t mm:latest --load .
```

## Запуск

### Локальный Hadoop

```bash
# Базовый запуск (1 группа, 1 reducer)
hadoop jar target/mm.jar mm A/ B/ C/

# С блочным разбиением и параллелизмом
hadoop jar target/mm.jar mm \
  -D mm.groups=4 \
  -D mapred.reduce.tasks=8 \
  -D mm.float-format="%.5f" \
  A/ B/ C/

# С конфигурационным файлом
hadoop jar target/mm.jar mm -conf config.xml A/ B/ C/
```

### Docker

```bash
# Запуск контейнера
docker run --rm -it --entrypoint bash \
  -v $(pwd)/data:/mm/data \
  mm:latest

# Внутри контейнера
/opt/hadoop-3.3.6/bin/hadoop jar target/mm.jar mm \
  -conf /mm/data/config.xml \
  /mm/data/A \
  /mm/data/B \
  /mm/data/out
```

## Алгоритм блочного умножения

### Математика

C = A × B разбивается на блоки:

```
C_ij = Σ(k) A_ik × B_kj
```

где i, j, k - индексы блоков.

### Пример с 2 группами

Матрица 4×4 разбивается на 4 блока 2×2:

```
A = [A_00  A_01]    B = [B_00  B_01]
    [A_10  A_11]        [B_10  B_11]

C_00 = A_00×B_00 + A_01×B_10
C_01 = A_00×B_01 + A_01×B_11
C_10 = A_10×B_00 + A_11×B_10
C_11 = A_10×B_01 + A_11×B_11
```

## Оптимизации

### 1. Разреженность

Программа автоматически пропускает нулевые элементы:
- Хранятся только ненулевые элементы
- В выводе нули не записываются

### 2. Блочное умножение

Параметр `mm.groups` контролирует размер блоков:
- `groups=1`: классическое умножение (нет блоков)
- `groups=N`: матрица разбивается на N×N блоков
- Оптимальное значение: √(количество reducers)

### 3. SequenceFile

Промежуточные данные сохраняются в бинарном формате SequenceFile для эффективности.

### 4. Статистика

Программа выводит детальную статистику:
- MAP_INPUT_RECORDS
- MAP_OUTPUT_RECORDS
- REDUCE_SHUFFLE_BYTES
- Время выполнения

## Производительность

### Пример: 800×800 × 800×600 (разреженность 1%)

| groups | reducers | Время | Shuffle | Output Records |
|--------|----------|-------|---------|----------------|
| 1      | 1        | 420s  | 3.8 GB  | 480000         |
| 2      | 4        | 180s  | 3.8 GB  | 480000         |
| 4      | 8        | 95s   | 3.8 GB  | 480000         |
| 8      | 16       | 65s   | 3.8 GB  | 480000         |

## Структура кода

```
mm/
├── src/main/java/
│   └── mm.java                    # Основной класс
│       ├── BlockWritable          # Writable для элемента блока
│       ├── retSizes              # Структура размеров
│       ├── MatrixBlockMapper     # Mapper прохода 1
│       ├── MatrixBlockAggregate  # Reducer прохода 1
│       ├── MatrixSumMapper       # Mapper прохода 2
│       └── MatrixSumAggregate    # Reducer прохода 2
└── pom.xml                        # Maven конфигурация
```

## Проверка корректности

### Генерация тестовых матриц

```bash
# Создать небольшие матрицы для проверки
cat > A/data <<EOF
A	0	0	1.0
A	0	1	2.0
A	1	0	3.0
A	1	1	4.0
EOF

echo "2	2" > A/size

cat > B/data <<EOF
B	0	0	5.0
B	0	1	6.0
B	1	0	7.0
B	1	1	8.0
EOF

echo "2	2" > B/size

# Запустить умножение
hadoop jar target/mm.jar mm A/ B/ C/

# Ожидаемый результат C:
# C[0,0] = 1*5 + 2*7 = 19
# C[0,1] = 1*6 + 2*8 = 22
# C[1,0] = 3*5 + 4*7 = 43
# C[1,1] = 3*6 + 4*8 = 50
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
