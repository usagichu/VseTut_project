/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Лысак Мария
 * Дата: 06.01.26

Часть 1. Разработка витрины данных */

-- Информация о промокодах, рассрочке, типе первого платежа.
-- Учтено payment_sequential может начинаться не с 1, поэтому первый платёж определяется как минимальный
WITH order_info AS (
 	SELECT 
 		u.user_id,
 		o.order_id,
 		MAX(CASE WHEN op.payment_sequential = (
             SELECT 
             	MIN(payment_sequential) 
             FROM ds_ecom.order_payments op2 
             WHERE op2.order_id = op.order_id
           ) AND op.payment_type='денежный перевод' THEN 1 ELSE 0 END) AS first_pay,
 		MAX(CASE WHEN payment_type='промокод' THEN 1 ELSE 0 END) AS promo_using,
 		MAX(CASE WHEN payment_installments>1 THEN 1 ELSE 0 END) AS installment_using
 	FROM ds_ecom.order_payments AS op
 	LEFT JOIN ds_ecom.orders AS o USING(order_id)
 	LEFT JOIN ds_ecom.users AS u USING(buyer_id)
 	WHERE o.order_status IN ('Доставлено', 'Отменено')
 	GROUP BY u.user_id, o.order_id
),
-- Стоимость доставленных заказов
order_cost AS (
	SELECT
		order_id,
		SUM(price) + SUM(delivery_cost) AS total_cost
	FROM (
		SELECT
			order_id
		FROM ds_ecom.orders AS o
		WHERE order_status = 'Доставлено' ) AS filter_orders
	LEFT JOIN ds_ecom.order_items AS oi USING(order_id)
	GROUP BY order_id
),
-- Рейтинг заказов с обработкой ошибки данных
-- Если отзывов на один заказ несколько, то находится среднее значение оценки
order_rating AS ( 
	SELECT 
		order_id,
		AVG(CASE 
			WHEN review_score>=10 THEN ROUND(review_score/10,2)
			ELSE review_score
		END) AS avg_review_score
	FROM ds_ecom.order_reviews
	GROUP BY order_id
), 
-- Информация о пользователях из топ-3 регионов со статусами заказов 'Доставлено', 'Отменено'
user_info AS ( 
	SELECT
		u.user_id,
		u.region,
		MIN(o.order_purchase_ts) AS first_order_ts,
		MAX(o.order_purchase_ts) AS last_order_ts,
		MAX(o.order_purchase_ts)-MIN(order_purchase_ts) AS lifetime,
		COUNT(DISTINCT o.order_id) AS total_orders,
		COUNT(DISTINCT o.order_id) FILTER(WHERE o.order_status='Отменено') AS num_canceled_orders,
		ROUND(COUNT(DISTINCT o.order_id) FILTER(WHERE o.order_status='Отменено') / COUNT(DISTINCT o.order_id)::numeric,2) AS canceled_orders_ratio
	FROM ds_ecom.users AS u
	JOIN ds_ecom.orders AS o  ON u.buyer_id = o.buyer_id AND o.order_status IN ('Доставлено', 'Отменено')
	WHERE u.region IN (
		SELECT 
			u.region
		FROM ds_ecom.users AS u
		LEFT JOIN ds_ecom.orders AS o USING(buyer_id)
		WHERE o.order_status IN ('Доставлено', 'Отменено')
		GROUP BY u.region
		ORDER BY COUNT(DISTINCT o.order_id) DESC
		LIMIT 3) 
	GROUP BY u.user_id, u.region
),
-- Фильтрация заказов и регионов
filter_user_order AS(
	SELECT 
		u.user_id,
		u.region,
		o.order_id,
		o.order_status
	FROM ds_ecom.users AS u
	LEFT JOIN ds_ecom.orders AS o USING(buyer_id)
	WHERE o.order_status IN ('Доставлено', 'Отменено') AND u.region IN (
		SELECT 
			u.region
		FROM ds_ecom.users AS u
		LEFT JOIN ds_ecom.orders AS o USING(buyer_id)
		WHERE o.order_status IN ('Доставлено', 'Отменено')
		GROUP BY u.region
		ORDER BY COUNT(DISTINCT o.order_id) DESC
		LIMIT 3)
)
SELECT
	ui.user_id,
	ui.region,
	ui.first_order_ts,
	ui.last_order_ts,
	ui.lifetime,
	ui.total_orders,
	ROUND(AVG(r.avg_review_score)::numeric,2) AS avg_order_rating,
	COUNT(DISTINCT r.order_id) FILTER(WHERE r.avg_review_score IS NOT NULL ) AS num_orders_with_rating,
	ui.num_canceled_orders,
	ui.canceled_orders_ratio,
	SUM(oc.total_cost) AS total_order_costs,
	ROUND(AVG(oc.total_cost), 2) AS avg_order_cost,
	SUM(oi.installment_using) AS num_installment_orders,
	SUM(oi.promo_using) AS num_orders_with_promo,
	MAX(oi.first_pay) AS used_money_transfer,
	MAX(oi.installment_using) AS used_installments,
	MAX(CASE WHEN ui.num_canceled_orders>0 THEN 1 ELSE 0 END) AS used_cancel
FROM user_info AS ui
JOIN filter_user_order AS fui USING(user_id,region)
LEFT JOIN order_rating  AS r USING(order_id)
LEFT JOIN order_cost  AS oc USING(order_id)
LEFT JOIN order_info AS oi USING(user_id,order_id)
GROUP BY ui.user_id,ui.region, ui.first_order_ts, ui.last_order_ts,ui.lifetime,ui.total_orders,ui.num_canceled_orders,ui.canceled_orders_ratio
ORDER BY user_id;
/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/


/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/

-- Напишите ваш запрос тут
WITH segments AS (
	SELECT
		user_id,
		total_orders,
		total_order_costs,
		CASE 
			WHEN total_orders=1 THEN '1 заказ'
			WHEN total_orders BETWEEN 2 AND 5 THEN '2-5 заказов'
			WHEN total_orders BETWEEN 6 AND 10 THEN '6-10 заказов'
			WHEN total_orders>=11 THEN '11 и более заказов'
		END AS segment
	FROM ds_ecom.product_user_features
)
SELECT 
	segment,
	COUNT(DISTINCT user_id) AS total_users,
	ROUND(AVG(total_orders),2) AS avg_orders,
	ROUND(SUM(total_order_costs) / SUM(total_orders),2) AS avg_cost
FROM segments
GROUP BY segment
ORDER BY MIN(total_orders);

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 
 Большая часть пользователей совершила только один заказ, а 11 и более заказов совершил только 1 пользователь.
 Чем больше количество заказов, тем меньше средняя стоимость одного заказа.

**/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/

SELECT 
	DENSE_RANK() OVER (ORDER BY avg_order_cost DESC) AS rank,
	user_id,
	region,
	total_orders,
	total_order_costs,
	avg_order_cost
FROM ds_ecom.product_user_features
WHERE total_orders>=3
ORDER BY avg_order_cost DESC
LIMIT 15;

/* Напишите краткий комментарий с выводами по результатам задачи 2.
 
  Среди пользователей, сделавших 3 заказа или более самый большой средний чек составляет 14716.67, 
  а наибольшая общая сумма заказов 44150.00. При этом всего 2 пользователя сделало больше 3 заказов.
  В первом задание средняя стоимость заказа в группе '2-5 заказов' была 3058.39, а в этом средний чек варьируется 
  от 5526.67 до 14716.67
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/
SELECT 
	region,
	COUNT(DISTINCT user_id) AS all_users,
	SUM(total_orders) AS all_orders,
	ROUND(AVG(avg_order_cost),2) AS avg_cost,
	ROUND(SUM(num_installment_orders)/SUM(total_orders)::numeric,3) AS part__installments,
	ROUND(SUM(num_orders_with_promo)/SUM(total_orders)::numeric,3) AS part_promo,
	ROUND(SUM(used_cancel)/COUNT(DISTINCT user_id)::numeric,3) AS part_cancel
FROM ds_ecom.product_user_features
GROUP BY region
ORDER BY all_users DESC;
/* Напишите краткий комментарий с выводами по результатам задачи 3.
 
 	Наибольшое число пользователей(40747) и заказов(39386) в Москве. Средняя стоимость заказа между тремя 
 	регионами примерно равна и находится в пределах 3167.5-3620.16, причем наибольшая в Санкт-Петербурге,
 	а наименьшая в Москве. Доля использования рассрочки(0.477-0.547), промокодов(0.037-0.042) и 
 	отмененных заказов(0.004-0.006) также примерно равна
 
*/



/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/
SELECT
	EXTRACT('MONTH' FROM first_order_ts)  AS date_month,
	COUNT(DISTINCT user_id) AS all_users,
	SUM(total_orders) AS all_orders,
	ROUND(AVG(avg_order_cost),2) AS avg_cost,
	ROUND(AVG(avg_order_rating),2) AS avg_rating,
	ROUND(SUM(used_money_transfer)/COUNT(DISTINCT user_id)::numeric,3) AS part_money_transfer,
	DATE_TRUNC('minute',AVG(lifetime)) AS avg_lifetime
FROM ds_ecom.product_user_features
WHERE EXTRACT('YEAR'FROM first_order_ts)=2023
GROUP BY date_month
ORDER BY date_month;
/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * 
	Пользователей, которые совершили первый заказ в 1 месяц 2023 меньше всего(465), а большего всего в 11 месяце(4703)
	и 12 месяце(3589)
	Аналогично, меньше всего заказов в 1 месяце(499), а больше всего в 11 месяце(4892) и 12 месяце(3696). Средняя стоимость заказов
	по месяцам в 2023 изменялась в пределах 2581.28-3311.92
	Средний рейтинг заказа почти не менялся за 2023 и находится в пределах 4-4.32
	Доля пользователей, использовавших денежные переводы также находится на примерно одинаковом уровне(0.19-0.221)
	Наибольшая продолжительность активности пользователя - 12 дней, а наименьшая 2 