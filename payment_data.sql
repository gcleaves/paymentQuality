select 
    p.A_W
    ,p.product
    ,p.source
    ,p.cohort
    ,p.payWeek
    ,sum(cs.subscribers) as originalSubscribers
    ,CASE WHEN p.cohort=p.payWeek THEN sum(cs.subscribers) ELSE 0 END as subscribers
    ,sum(p.payments) as payments
    ,CASE WHEN p.cohort=p.payWeek THEN sum(p.payments) ELSE 0 END as payers
    ,(DATEDIFF(p.payWeek,p.cohort) / 7) + 1 as weeks
    ,sum(cs.subscribers) * ((CAST(DATEDIFF(p.payWeek,p.cohort) AS SIGNED) / 7) + 1) as possiblePayments
from (
    Select 
        lower(sub.product) product,
        lower(tr.source) source,
        dsub.year_week_start cohort,
        dpay.year_week_start payWeek,
        count(distinct pay.id) payments,
        CASE tr.affiliate WHEN 1 THEN "Affiliate" ELSE "Webmaster" END as A_W
    from
        simplemvas.dcb_subscriptors as sub
                inner join
        general.dates dsub ON date(sub.subStartDate) = dsub.date
                left join
        simplemvas.dcb_payments as pay ON sub.request_id2 = pay.request_id2 and pay.status = 2
                left join
        general.dates dpay ON date(pay.payment_date) = dpay.date
                inner join
        simplemvas.dcb_transactions t ON sub.request_id2 = t.request_id2
                inner join
        tracking.entrance e ON e.id = t.vid
                inner join
        tracking.traffic_rates tr ON tr.id = e.trafficrateid
    where
        sub.status != - 1
        and dsub.year_week_start != (CURDATE() - INTERVAL WEEKDAY(CURDATE()) DAY)
        and dpay.year_week_start != (CURDATE() - INTERVAL WEEKDAY(CURDATE()) DAY)
        #and sub.product in ('videospremium')
        #and tr.source = 'adcash'
        #and sub.subStartDate >= '2015-01-19'
    group by lower(sub.product),lower(tr.source),cohort,payWeek
    order by lower(sub.product),lower(tr.source),cohort,payWeek
) p
inner join _tmp.cohort_subs cs on cs.cohort=p.cohort and lower(cs.product)=lower(p.product) and lower(cs.source)=lower(p.source)
where p.payWeek is not null
group by lower(p.product), lower(p.source), p.cohort, p.payWeek
order by lower(p.product), lower(p.source), p.cohort, p.payWeek # ORDER is important to php script! 