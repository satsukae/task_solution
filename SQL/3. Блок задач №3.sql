/* PostgreSQL 15.3

В базе данных PG (Postgre) cуществуют 2 таблицы:

1. Таблица "opers", с операциями по обработке нарушений. В ней хранится информация по всем операциям обработки нарушений.
  |       Column       |              Type              | Description
  ---------------------+--------------------------------+--------------------------------------------------------+
  |   viol_oper_id     | bigint (PK)                    | Идентификатор операции обработки нарушения ПДД
  |   tr_viol_id       | bigint                         | Идентификатор нарушения ПДД
  |   oper_code        | integer                        | Тип операции обработки нарушения ПДД
  |   refuse_code      | integer                        | Результирующий код операции обработки нарушения ПДД
  |   isp_id           | bigint                         | Идентификатор того, кто произвел обработку нарушения ПДД
  |   date_oper        | timestamp(0) without time zone | Дата и время операции обработки нарушения ПДД
  |   viol_datetime    | timestamp(0) without time zone | Дата и время нарушения ПДД

2. Таблица "viols", с нарушениями правил дорожного движения транспортными средствами.

  |   Column    |              Type              | Description
  --------------+--------------------------------+--------------------------------------------------+
  | tr_viol_id  | bigint (PK)                    | Идентификатор нарушения
  | viol_code   | integer                        | Код нарушения ПДД
  | time_check  | timestamp(0) without time zone | Дата и время нарушения ПДД
  | stage_code  | integer                        | Этап обработки нарушения ПДД
  | refuse_code | integer                        | Результирующий код обработки нарушения ПДД
  | camera_id   | integer                        | Идентификатор камеры зафиксировавший нарушение ПДД


Есть задача:
Подготовить выгрузку, содержащую информацию, за период нарушения ПДД с 2018-04-01 по 2018-04-20 с детализацией до даты нарушения. Выгрузка должна содержать атрибуты:
    Дата нарушения ПДД;
    Число нарушений ПДД всего;
    Число нарушений ПДД у которых результирующий код обработки нарушения ПДД равен 0;
    Число нарушений ПДД по которым была операция обработки тип 29;
    Число нарушений ПДД по которым не было операции обработки тип 29, а результирующий код обработки нарушения ПДД равен 0;


Вам необходимо:
1. Описать последовательность действий для выполнения задачи, постарайтесь развернуто ответить почему так и в такой последовательности.
2. Для этой задачи подготовьте все варианты запросов, которые позволят произвести соответствующую выгрузку.
3. Укажите лучший вариант запроса для данной задачи, дайте комментарий почему именно такой запрос.

Ход решения:

Для начала необходимо запуспить "create_script", который создает две таблицы opers, viols.

Затем необходимо запустить "generate_script" (данные сгенерированы c помощью конструктора на generategata.com), 
который заполняет две таблицы данными (100 строк).

Выполняю запросы, команда DATE() необходима для извчлечения даты без времени, 
порядок атрибутов, соотвествует условию, с помощью команды EXPLAIN ANALYZE отслеживаю затраченное время и память для каждого решения.
*/

--РЕШЕНИЕ №1
SELECT 
	DATE(o.viol_datetime) AS date,
	COUNT(o.tr_viol_id) AS amount_all,
	COUNT(CASE WHEN v.refuse_code = 0 THEN 1 END) AS amount_refuse_0,
	COUNT(CASE WHEN o.oper_code = 29 THEN 1 END) AS amount_code_29,
	COUNT(CASE WHEN o.oper_code != 29 AND v.refuse_code = 0 THEN 1 END) AS amount_not_29_refuse_0
FROM opers o
JOIN viols v ON o.tr_viol_id=v.tr_viol_id 
WHERE o.viol_datetime BETWEEN '2018-04-01' AND '2018-04-20'
GROUP BY 1 
ORDER BY 1;
--Execution Time: 0.137 ms


--РЕШЕНИЕ №2
SELECT 
	DATE(o.viol_datetime) AS date,
	SUM(CASE WHEN o.tr_viol_id = o.tr_viol_id THEN 1 END) AS amount_all,
	SUM(CASE WHEN v.refuse_code = 0 THEN 1 ELSE 0 END) AS amount_refuse_0,
	SUM(CASE WHEN o.oper_code = 29 THEN 1 ELSE 0 END) AS amount_code_29,
	SUM(CASE WHEN o.oper_code != 29 AND v.refuse_code = 0 THEN 1 ELSE 0 END) AS amount_not_29_refuse_0
FROM opers o
JOIN viols v ON o.tr_viol_id=v.tr_viol_id 
WHERE o.viol_datetime BETWEEN '2018-04-01' AND '2018-04-20'
GROUP BY 1 
ORDER BY 1;
--Execution Time: 0.135 ms


--РЕШЕНИЕ №3
WITH pivot_pdd AS(
	SELECT
		DATE(o.viol_datetime) AS date,
		COUNT(o.tr_viol_id) AS amount_all,
		COUNT(CASE WHEN v.refuse_code = 0 THEN 1 END) AS amount_refuse_0,
		COUNT(CASE WHEN o.oper_code = 29 THEN 1 END) AS amount_code_29,
		COUNT(CASE WHEN o.oper_code != 29 AND v.refuse_code = 0 THEN 1 END) AS amount_not_29_refuse_0
	FROM opers o
	JOIN viols v ON o.tr_viol_id=v.tr_viol_id 
	WHERE o.viol_datetime BETWEEN '2018-04-01' AND '2018-04-20'
	GROUP BY 1
	ORDER BY 1)
SELECT 
	date, amount_all, amount_refuse_0, amount_code_29, amount_not_29_refuse_0
FROM pivot_pdd;
--Execution Time: 0.167 ms

--РЕШЕНИЕ №4
CREATE VIEW pivot_pdd  AS
	SELECT
		DATE(o.viol_datetime) AS date,
		COUNT(o.tr_viol_id) AS amount_all,
		COUNT(CASE WHEN v.refuse_code = 0 THEN 1 END) AS amount_refuse_0,
		COUNT(CASE WHEN o.oper_code = 29 THEN 1 END) AS amount_code_29,
		COUNT(CASE WHEN o.oper_code != 29 AND v.refuse_code=0 THEN 1 END) AS amount_not_29_refuse_0
	FROM opers o
	JOIN viols v ON o.tr_viol_id=v.tr_viol_id 
	WHERE o.viol_datetime BETWEEN '2018-04-01' AND '2018-04-20'
	GROUP BY 1
	ORDER BY 1

SELECT 
	date, amount_all, amount_refuse_0, amount_code_29, amount_not_29_refuse_0
FROM pivot_pdd;
--Execution Time: 0.286 ms


--РЕШЕНИЕ №5
EXPLAIN ANALYZE
SELECT 
	DATE(o.viol_datetime) AS date,
	COUNT(o.tr_viol_id) AS amount_all,
	COUNT(v.refuse_code) FILTER (WHERE v.refuse_code = 0) AS amount_refuse_0,
	COUNT(o.oper_code) FILTER (WHERE o.oper_code = 29) AS amount_code_29,
	COUNT(o.oper_code) FILTER (WHERE o.oper_code != 29 AND v.refuse_code = 0) AS amount_not_29_refuse_0
FROM opers o
JOIN viols v ON o.tr_viol_id=v.tr_viol_id 
WHERE o.viol_datetime BETWEEN '2018-04-01' AND '2018-04-20'
GROUP BY 1 
ORDER BY 1;
--Execution Time: 0.141 ms

/*
Решения 1 и 2 реализованы через оператор CASE с разными агрегирующими функциями, SUM немного быстрее выполняет запрос, но это доли милисекунд.
Решение 3 представляет собой обобщенное табличное выражение (CTE), выполняется на 0.03-0.032 ms дольше по сравнению с первми двумя.
Решение 4 это представление, занимает место, дольше выполняется, но пригодится для посроения отчета (при повторном использовании).
Решение 5 в PostgreSQL реализована команда Filter, синтаксис несложный, выполняет запрос чуть медленее относительно 1 и 2 решения.

ВЫВОД: лучше всего использовать решение №2, так как оно самое эффективное, но при условии неоднократного обращения в скрипте к выгрузке,
	можно использовать решение №3, т.к. оно удобнее
*/
