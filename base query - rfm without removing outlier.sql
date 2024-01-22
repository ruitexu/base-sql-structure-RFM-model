--DECLARE @lyyearend DATE = (SELECT dateadd(day, -366, cast(getdate() as date)));

--DECLARE @lyyearstart DATE = (SELECT dateadd(day, -731, cast(getdate() as date)));

--DECLARE @yearend DATE = (SELECT cast(getdate() as date));

--DECLARE @yearstart DATE = (SELECT dateadd(day, -365, cast(getdate() as date)));


DECLARE @yearend DATE = (SELECT max([date])
  FROM [dbo].[FiscalCalendar]
  where [quarter] = (select [quarter] from [FiscalCalendar] where cast(date as date) = cast(getdate() as date)) - 1
  and fiscalYear = (select fiscalYear from [FiscalCalendar] where cast(date as date) = cast(getdate() as date)));

DECLARE @yearstart DATE = (SELECT dateadd(day, -365, @yearend));

DECLARE @lyyearend DATE = (SELECT dateadd(day, -366, @yearend));

DECLARE @lyyearstart DATE = (SELECT dateadd(day, -731, @yearend));


   with TransactionSummary AS (
    SELECT
        c.customerNumber,
        MIN(ct.transactionDate) AS minTransactionDate,
        MAX(ct.transactionDate) AS maxTransactionDate
    FROM
        [dbo].[CRMCustomer] c
    LEFT JOIN
        CRMTransactions ct ON ct.customerNumber = c.customerNumber
    GROUP BY
        c.customerNumber
)
, cx as (
SELECT
    c.customerId,
    c.customerNumber,
    c.languageCode,
    c.bitrhDate,
    COALESCE(ts.minTransactionDate, [firstTransactionDate]) AS firstTransactionDate,
    c.firstTransactionStore,
    COALESCE(ts.maxTransactionDate, [lastTransactionDate]) AS lastTransactionDate,
    c.lastTransactionStore,
    f1.fiscalYear AS firstTransactionFiscalYear,
    f1.fiscalWeek AS firstTransactionFiscalWeek,
    f1.month AS firstTransactionFiscalPeriod,
    f1.quarter AS firstTransactionFiscalQuarter,
    f2.fiscalYear AS lastTransactionFiscalYear,
    f2.fiscalWeek AS lastTransactionFiscalWeek,
    f2.month AS lastTransactionFiscalPeriod,
    f2.quarter AS lastTransactionFiscalQuarter
FROM
    [dbo].[CRMCustomer] c
LEFT JOIN
    TransactionSummary ts ON c.customerNumber = ts.customerNumber
LEFT JOIN
    [dbo].[FiscalCalendar] f1 ON f1.date = COALESCE(ts.minTransactionDate, [firstTransactionDate])
LEFT JOIN
    [dbo].[FiscalCalendar] f2 ON f2.date = COALESCE(ts.maxTransactionDate, [lastTransactionDate]))
  
  ,store_sales as (SELECT 
      --case when transactionStore = '993' then 'Ecom' else 'Store' end as Channel
	  'Store' as Channel
	  --,division as Banner
	  ,case when division like '%Melanie%' then 'Melanie Lyne' else 'Laura' end as Banner
	  ,c.customerNumber
	  ,[firstTransactionDate]
      ,[firstTransactionStore]
      ,[lastTransactionDate]
      ,[lastTransactionStore]
	  ,firstTransactionFiscalYear
	  ,firstTransactionFiscalWeek
	  ,firstTransactionFiscalPeriod
	  ,firstTransactionFiscalQuarter
	  ,lastTransactionFiscalYear
	  ,lastTransactionFiscalWeek
	  ,lastTransactionFiscalPeriod
	  ,lastTransactionFiscalQuarter
	  ,transactionDate
	  ,CONCAT(c.customerNumber,transactionDate) as 'cx_trxn_date'
	  ,f.fiscalYear as fiscalYear
	  ,f.fiscalWeek as fiscalWeek
	  ,f.Month as fiscalPeriod
	  ,f.quarter as fiscalQuarter
      ,[TransactionID]
      ,unitRetail as sales
      ,unitCost as cost
      ,unitQuantity as unit
	  ,[saleOrReturnIndicator]
	  ,transactionType
	  ,CONCAT(classcode,colourCode,styleAKA) as sku
  FROM [dbo].[CRMTransactions] s
  left join [dbo].[Store] st on cast(st.storeNumber as int) = transactionStore
  left join [dbo].[FiscalCalendar] f on f.date  = transactionDate
  left join cx c on c.customerNumber = s.customerNumber
  where transactionDate between @yearstart and @yearend 
  and 
  transactionStore <> '993'
  
  )

  ,ecom_sales as (SELECT 
      --case when transactionStore = '993' then 'Ecom' else 'Store' end as Channel
	  'Ecom' as Channel
	  --,division as Banner
	  ,case when left(classCode, 2) = 60 then 'Melanie Lyne' else 'Laura' end as Banner
	  ,c.customerNumber
	  ,[firstTransactionDate]
      ,[firstTransactionStore]
      ,[lastTransactionDate]
      ,[lastTransactionStore]
	  ,firstTransactionFiscalYear
	  ,firstTransactionFiscalWeek
	  ,firstTransactionFiscalPeriod
	  ,firstTransactionFiscalQuarter
	  ,lastTransactionFiscalYear
	  ,lastTransactionFiscalWeek
	  ,lastTransactionFiscalPeriod
	  ,lastTransactionFiscalQuarter
	  ,transactionDate
	  ,CONCAT(c.customerNumber,transactionDate) as 'cx_trxn_date'
	  ,f.fiscalYear as fiscalYear
	  ,f.fiscalWeek as fiscalWeek
	  ,f.month as fiscalPeriod
	  ,f.quarter as fiscalQuarter
      ,[TransactionID]
      ,unitRetail as sales
      ,unitCost as cost
      ,unitQuantity as unit
	  ,[saleOrReturnIndicator]
	  ,transactionType
	  ,CONCAT(classcode,colourCode,styleAKA) as sku
  FROM [dbo].[CRMTransactions] s
  --left join [dbo].[Store] st on cast(st.storeNumber as int) = transactionStore
  left join [dbo].[FiscalCalendar] f on f.date  = transactionDate
  left join cx c on c.customerNumber = s.customerNumber
  where transactionDate between @yearstart and @yearend
  and 
  transactionStore = '993'
  
  )


  ,total_sales as (select * from ecom_sales
  union all
  select * from store_sales
)

,cx_spend as (select sum(sales) as spend,
count(distinct cx_trxn_date) as visit,
count(distinct case when transactionType = 'S' then cx_trxn_date else Null end) as sales_only_visit,
count(distinct case when transactionType = 'R' then cx_trxn_date else Null end) as return_visit,
sum(unit) as unit,
sum(cost) as cost,
sum(sales) - sum(cost) as GM,
sum(case when [saleOrReturnIndicator] = 'R' then sales else 0 end) as return_amount,
sum(case when Channel = 'Ecom' then sales else 0 end) as ecom_spend,
sum(case when Channel = 'Store' then sales else 0 end) as store_spend,
DATEDIFF(month, case when [lastTransactionDate] is not null then [lastTransactionDate] else max(transactionDate) end, @yearend) as recency,
count(case when transactionType = 'S' then sku else NUll end) - count(distinct case when transactionType = 'S' then sku else NUll end) as duplication_identifier,
case when firstTransactionDate >= @yearstart then 1 else 0 end as is_new,
customerNumber

from total_sales
group by customerNumber,[lastTransactionDate],firstTransactionDate)


, CustomerWithRank as (SELECT
  customerNumber,
  is_new,
  visit,
  spend,
  ecom_spend,
  store_spend,
  unit,
  cost,
  GM,
  return_amount,
  recency,
  duplication_identifier,
  sales_only_visit,
  return_visit,
  1.0*spend/1.0*visit as spend_per_visit,
  sales_only_visit*1.0/visit*1.0 as sales_percent,
  1.0*duplication_identifier/NULLIF(1.0*unit,0) as duplication_perc,
  RANK() OVER (ORDER BY spend DESC) AS rank,
  SUM(spend) OVER (ORDER BY spend DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_spend
FROM
  cx_spend

  )

, DecileThresholds AS (
  SELECT
    SUM(spend) / 10 AS first_decile,
    2 * SUM(spend) / 10 AS second_decile,
    3 * SUM(spend) / 10 AS third_decile,
    4 * SUM(spend) / 10 AS fourth_decile,
    5 * SUM(spend) / 10 AS fifth_decile,
    6 * SUM(spend) / 10 AS sixth_decile,
    7 * SUM(spend) / 10 AS seventh_decile,
    8 * SUM(spend) / 10 AS eighth_decile,
    9 * SUM(spend) / 10 AS ninth_decile
  FROM
    CustomerWithRank
)

,final as (SELECT
  cr.customerNumber,
  cr.is_new,
  cr.spend,
  cr.ecom_spend,
  cr.store_spend,
  cr.recency,
  cr.cost,
  cr.unit,
  cr.GM,
  cr.visit,
  cr.sales_only_visit,
  cr.sales_percent,
  duplication_perc,
  cr.spend_per_visit,
  cr.return_amount,
  cr.rank,
  cr.return_visit,
  CASE
    WHEN cr.cumulative_spend <= dt.first_decile THEN 1
    WHEN cr.cumulative_spend <= dt.second_decile THEN 2
    WHEN cr.cumulative_spend <= dt.third_decile THEN 3
    WHEN cr.cumulative_spend <= dt.fourth_decile THEN 4
    WHEN cr.cumulative_spend <= dt.fifth_decile THEN 5
    WHEN cr.cumulative_spend <= dt.sixth_decile THEN 6
    WHEN cr.cumulative_spend <= dt.seventh_decile THEN 7
    WHEN cr.cumulative_spend <= dt.eighth_decile THEN 8
    WHEN cr.cumulative_spend <= dt.ninth_decile THEN 9
    ELSE 10
  END AS decile
FROM
  CustomerWithRank cr
  CROSS JOIN DecileThresholds dt)


SELECT
  decile,
  COUNT(DISTINCT customerNumber) AS cx,
  SUM(spend) AS total_spend,
  AVG(spend) AS avg_spend,
  MIN(spend) AS min_spend,
  MAX(spend) AS max_spend,
  COUNT(DISTINCT CASE WHEN ecom_spend > 0 AND store_spend <= 0 THEN customerNumber ELSE NULL END) AS 'Ecom_only_cx',
  COUNT(DISTINCT CASE WHEN ecom_spend <= 0 AND store_spend > 0 THEN customerNumber ELSE NULL END) AS 'Store_only_cx',
  COUNT(DISTINCT CASE WHEN ecom_spend > 0 AND store_spend > 0 THEN customerNumber ELSE NULL END) AS 'Omni_cx',
  COUNT(DISTINCT CASE WHEN ecom_spend < 0 AND store_spend < 0 THEN customerNumber ELSE NULL END) AS 'negative_spend_cx',
  SUM(cost) AS cost,
  SUM(unit) AS unit,
  SUM(GM) AS GM,
  SUM(GM) / SUM(spend) AS 'GM%',
  MIN(visit) AS min_visit,
  MAX(visit) AS max_visit,
  AVG(visit) AS avg_visit,
  SUM(visit) AS visit,
  SUM(return_amount) AS return_amount,
  AVG(CASE WHEN spend <> 0 THEN -1 * return_amount / spend ELSE NULL END) AS 'avg_return_%age',
  COUNT(DISTINCT CASE WHEN recency < 3 THEN customerNumber ELSE NULL END) AS '0-3 month',
  COUNT(DISTINCT CASE WHEN recency BETWEEN 3 AND 5.999 THEN customerNumber ELSE NULL END) AS '3-6 month',
  COUNT(DISTINCT CASE WHEN recency BETWEEN 6 AND 8.999 THEN customerNumber ELSE NULL END) AS '6-9 month',
  COUNT(DISTINCT CASE WHEN recency >= 9 THEN customerNumber ELSE NULL END) AS '9-12 month',
  sum(return_visit) as return_visit,
  COUNT(DISTINCT case when is_new = 1 then customerNumber else null end) AS new_cx,
  sum(CASE WHEN ecom_spend > 0 AND store_spend <= 0 THEN spend ELSE 0 END) AS 'Ecom_only_revenue',
  sum(CASE WHEN ecom_spend <= 0 AND store_spend > 0 THEN spend ELSE 0 END) AS 'Store_only_revenue',
  sum(CASE WHEN ecom_spend > 0 AND store_spend > 0 THEN spend ELSE 0 END) AS 'Omni_revenue',
  sum(CASE WHEN ecom_spend > 0 AND store_spend <= 0 THEN visit ELSE 0 END) AS 'Ecom_only_visit',
  sum(CASE WHEN ecom_spend <= 0 AND store_spend > 0 THEN visit ELSE 0 END) AS 'Store_only_visit',
  sum(CASE WHEN ecom_spend > 0 AND store_spend > 0 THEN visit ELSE 0 END) AS 'Omni_visit',

  COUNT(DISTINCT CASE WHEN ecom_spend > 0 AND store_spend <= 0 and visit >= 2 THEN customerNumber ELSE NULL END) AS '2+trxn_Ecom_only_cx',
  COUNT(DISTINCT CASE WHEN ecom_spend <= 0 AND store_spend > 0 and visit >= 2 THEN customerNumber ELSE NULL END) AS '2+trxn_Store_only_cx',
  COUNT(DISTINCT CASE WHEN ecom_spend > 0 AND store_spend > 0 and visit >= 2 THEN customerNumber ELSE NULL END) AS '2+trxn_Omni_cx',
  sum(CASE WHEN ecom_spend > 0 AND store_spend <= 0 and visit >= 2 THEN spend ELSE 0 END) AS '2+trxn_Ecom_only_revenue',
  sum(CASE WHEN ecom_spend <= 0 AND store_spend > 0 and visit >= 2 THEN spend ELSE 0 END) AS '2+trxn_Store_only_revenue',
  sum(CASE WHEN ecom_spend > 0 AND store_spend > 0 and visit >= 2 THEN spend ELSE 0 END) AS '2+trxn_Omni_revenue',
  sum(CASE WHEN ecom_spend > 0 AND store_spend <= 0 and visit >= 2 THEN visit ELSE 0 END) AS '2+trxn_Ecom_only_visit',
  sum(CASE WHEN ecom_spend <= 0 AND store_spend > 0 and visit >= 2 THEN visit ELSE 0 END) AS '2+trxn_Store_only_visit',
  sum(CASE WHEN ecom_spend > 0 AND store_spend > 0 and visit >= 2 THEN visit ELSE 0 END) AS '2+trxn_Omni_visit'

FROM final
GROUP BY decile
ORDER BY decile ASC;