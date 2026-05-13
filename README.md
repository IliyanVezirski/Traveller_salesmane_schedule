# Sales PVRP Scheduler

Desktop приложение и Python backend за планиране на месечни посещения на търговски представители. Системата разпределя клиентите по дни, пази правилата за честота на посещение и използва OSRM/PyVRP за по-компактни маршрути.

Проектът е насочен към реален работен сценарий: много клиенти, няколко търговски представители, 4-седмичен цикъл, дневен капацитет и нужда маршрутите да не обикалят целия район всеки ден.

## Основни Възможности

- GUI приложение за Windows с табове `Input`, `Parameters`, `Validation`, `Run`, `Results` и `Logs`.
- CLI режим чрез `python main.py`.
- Вход от Excel или CSV.
- Нов входен формат с една `gps` колона: `42.69804,23.31229`.
- Обратна съвместимост със стария формат `lat` + `lon`.
- Валидация на входните данни преди оптимизация.
- OSRM road-distance матрици с chunking за големи групи клиенти.
- Cache на OSRM матрици, за да не се пресмятат всеки път.
- Haversine fallback, когато OSRM не е наличен.
- OSRM-базирано клъстериране чрез k-medoids.
- Дневни територии по търговец, с наказание за разтеглени пътни зони.
- Глобална география за анализ, без местене на клиенти между търговци.
- Селективно дневно планиране: PyVRP избира най-добрите клиенти за деня от позволен pool.
- Финално PyVRP подреждане на дневния маршрут.
- HTML карта с филтри по ден, седмица, търговец, локална и глобална територия.
- Excel export с финален график, дневни маршрути, summary, validation, coverage и параметри.
- Windows one-folder build чрез PyInstaller.

## Как Работи Логиката

Текущата логика не е класически route-first solver, който предварително генерира фиксирани маршрути и после избира между тях. Работи по-близо до client-day модел:

1. Зарежда входния файл и нормализира координатите.
2. Ако има `gps`, автоматично го разделя вътрешно на `lat` и `lon`.
3. Валидира задължителни колони, координати, честоти и капацитет.
4. За всеки търговски представител строи OSRM distance matrix.
5. Прави OSRM-базирани клиентски клъстери.
6. Разпределя клъстерите към дневни територии, като балансира посещенията и наказва дълги пътни span-ове.
7. За всеки ден създава pool от допустими клиенти.
8. Задължителните клиенти за деня влизат твърдо.
9. PyVRP избира допълнителни клиенти от pool-а до дневния капацитет.
10. Избраните клиенти се махат от следващи възможности според честотата и правилата.
11. Накрая PyVRP подрежда реалния дневен маршрут.

Това позволява на PyVRP да има избор между повече клиенти, вместо да подрежда вече заключен маршрут без свобода.

## Правила За Посещения

Поддържаните честоти са:

- `2`: два пъти месечно.
- `4`: веднъж седмично.
- `8`: два пъти седмично.

Активните consistency правила са:

- Клиент с честота `2` се държи в един и същ weekday за двете си посещения, когато е възможно.
- Клиент с честота `4` се посещава в един и същ weekday всяка седмица.
- Клиент с честота `8` се държи в двойка weekdays през седмиците.
- `fixed_weekday` ограничава клиента до конкретен ден или дни.
- `forbidden_weekdays` забранява дни.
- `preferred_weekdays` дава меко предпочитание, но не е твърдо правило.

Клиентите не се местят между търговски представители. Географията оптимизира само вътре в района на съответния търговец.

## Входен Файл

Използвайте `data/input_clients_template.xlsx` като референция.

Задължителни колони:

- `client_id`
- `client_name`
- `sales_rep`
- `gps`
- `visit_frequency`

Пример за `gps`:

```text
42.69804,23.31229
```

Опционални колони:

- `fixed_weekday`
- `forbidden_weekdays`
- `preferred_weekdays`
- `cluster_manual`
- `notes`

Старият формат с отделни `lat` и `lon` колони все още се поддържа, но sample файловете вече са в новия `gps` формат.

## Output Файлове

По подразбиране резултатите се записват в:

```text
output/
  final_schedule.xlsx
  maps/
    final_schedule_map.html
```

Excel workbook-ът съдържа:

- `Final_Schedule`: всички планирани посещения, включително GPS-derived координати, ред в маршрута, километри и метод.
- `Daily_Routes`: дневни маршрути с `distance_from_previous_km` и `cumulative_km`.
- `Summary_By_Sales_Rep`: обобщение по търговски представител.
- `Summary_By_Day`: обобщение по ден.
- `Validation`: входни и изходни проверки.
- `Candidate_Routes_Selected`: избраните дневни client sets.
- `Candidate_Coverage`: покритие по клиент.
- `Clients_Geography`: локална и глобална територия по клиент.
- `Parameters`: използваната конфигурация.

HTML картата позволява филтриране по:

- търговски представител;
- weekday;
- week index;
- day index;
- локална дневна територия;
- глобална територия.

## Основни Параметри

### `daily_route`

```yaml
daily_route:
  target_clients: 23
  min_clients: 14
  max_clients: 27
  allow_underfilled: true
  allow_overfilled: false
```

Контролира нормалния и максималния брой клиенти в дневен маршрут.

### `clustering`

```yaml
clustering:
  use_distance_matrix: true
  k_medoids_max_iterations: 30
  target_cluster_size: 4
  max_clusters_per_rep: 60
```

Определя колко фини да са географските клъстери. По-малък `target_cluster_size` дава по-ситна карта и повече свобода за компактни дни.

### `territory_days`

```yaml
territory_days:
  enabled: true
  scope: per_rep
  use_distance_matrix: true
  max_daily_territory_km: 75
  route_span_weight: 25
  route_span_over_limit_weight: 5000
  local_refinement_iterations: 25
```

Определя как клъстерите се групират към weekdays. Това е ключово за географската компактност. Ако денят започне да се разтяга, увеличете `route_span_over_limit_weight` или намалете `max_daily_territory_km`.

### `selective_day_routing`

```yaml
selective_day_routing:
  enabled: true
  compactness_strength: 1.0
  pool_size: 45
  territory_mismatch_penalty: 40000
  distance_penalty: 2000
```

Контролира дневния pool и колко силно се предпочитат близки клиенти. По-висок `compactness_strength` прави дните по-сбити, но може да намали гъвкавостта.

### `osrm`

```yaml
osrm:
  url: http://localhost:5000
  use_osrm: true
  use_cache: true
  fallback_to_haversine: true
  max_table_locations: 100
```

OSRM се използва за пътни разстояния. Ако OSRM не е достъпен и fallback е включен, приложението продължава с приблизителни haversine разстояния.

### `route_costing`

```yaml
route_costing:
  final_method: pyvrp
  route_type: open
  pyvrp_time_limit_seconds: 3
```

Финалното подреждане на всеки дневен маршрут се прави с PyVRP, когато е наличен. При проблем има fallback към `nearest_neighbor_2opt`.

## Стартиране С GUI

Инсталиране:

```bat
python -m pip install -r requirements.txt
python -m pip install -r requirements-gui.txt
python -m pip install -r requirements-optional.txt
```

Стартиране:

```bat
python run_gui.py
```

Работен flow:

1. В `Input` изберете Excel/CSV файл.
2. Натиснете `Зареди данни`.
3. Проверете `Validation`.
4. Настройте параметрите в `Parameters`.
5. Стартирайте от `Run`.
6. Отворете Excel, HTML карта или output папката от `Results`.

## Стартиране С CLI

Бърз smoke run без OSRM:

```bat
python main.py --input data/sample_clients.xlsx --output output/cli_test --no-osrm --no-cache --quiet-solver --time-limit 20 --num-workers 4 --target-clients 6 --min-clients 1 --max-clients 8
```

Production-style run:

```bat
python main.py --input data/input_clients_template.xlsx --config config.yaml --output output
```

Проверка на OSRM:

```bat
python main.py --check-osrm
```

## Build На Windows EXE

Инсталирайте build dependencies:

```bat
python -m pip install -r requirements-dev.txt
```

Стартирайте:

```bat
scripts\build_exe.bat
```

Готовият build е в:

```text
dist/SalesPVRP/SalesPVRP.exe
```

Build скриптът копира `config.yaml`, `README_USER.md`, `data/input_clients_template.xlsx` и `data/sample_clients.xlsx` в `dist/SalesPVRP`.

## Тестове

Инсталирайте development dependencies:

```bat
python -m pip install -r requirements-dev.txt -r requirements-optional.txt
```

Пълен test suite:

```bat
python -m pytest
```

Smoke script:

```bat
python scripts/smoke_test.py
```

## Troubleshooting

### `400 Client Error` от OSRM

Проверете дали coordinates се подават като `lon,lat` към OSRM. Входният Excel трябва да е `gps` във формат `lat,lon`, например `42.69804,23.31229`; приложението обръща реда вътрешно за OSRM.

### `No feasible solution`

Най-чести причини:

- твърде много задължителни клиенти в един ден;
- прекалено нисък `max_clients`;
- твърде много `fixed_weekday` ограничения;
- много забранени дни;
- район с нужни посещения над месечния капацитет.

Първи параметри за проба:

- увеличете `daily_route.max_clients`;
- намалете `daily_route.min_clients`;
- оставете `daily_route.allow_underfilled: true`;
- намалете `selective_day_routing.compactness_strength`;
- увеличете `selective_day_routing.pool_size`.

### Маршрутите са прекалено разтеглени

Параметри за стягане:

- намалете `territory_days.max_daily_territory_km`;
- увеличете `territory_days.route_span_over_limit_weight`;
- увеличете `selective_day_routing.compactness_strength`;
- намалете `clustering.target_cluster_size`.

### Excel не се презаписва

Затворете `final_schedule.xlsx` в Excel и пуснете оптимизацията отново.

### OSRM не работи

За тестове може да използвате:

```bat
python main.py --no-osrm
```

За production резултати стартирайте OSRM server и оставете `osrm.use_osrm: true`.

## Структура На Проекта

```text
src/
  pipeline.py                 public GUI/CLI pipeline
  data_loader.py              Excel/CSV loading and GPS parsing
  validation.py               input validation
  osrm_matrix.py              OSRM/haversine distance matrices
  clustering.py               OSRM k-medoids and weekday territories
  selective_day_scheduler.py  client-day selective scheduler
  final_routing.py            PyVRP final route ordering
  export_excel.py             workbook export
  map_visualization.py        HTML map export
gui/
  PySide6 desktop UI
data/
  input_clients_template.xlsx
  sample_clients.xlsx
scripts/
  build_exe.bat
  build_sample_workbooks.mjs
  convert_input_workbooks_to_gps.mjs
tests/
  pytest suite
```

## Бележки

- `global_geography` е аналитичен слой и не мести клиенти между търговци.
- Основната оптимизация е по търговец, за да се пази собствеността на районите.
- `lat` и `lon` са вътрешни работни колони след зареждане, дори входът да е само `gps`.
- Output файловете не са част от source control и могат да се регенерират.
