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
    ,(DATEDIFF(p.payWeek,p.cohort)) + 1 as weeks
    ,sum(cs.subscribers) * ((CAST(DATEDIFF(p.payWeek,p.cohort) AS SIGNED) / 7) + 1) as possiblePayments
from (
    Select 
        lower(sub.product) product,
        lower(t.source) source,
        DATE(sub.subStartDate) cohort,
        DATE(pay.payment_date) payWeek,
        count(distinct pay.id) payments,
        #CASE tr.affiliate WHEN 1 THEN "Affiliate" ELSE "Webmaster" END as A_W
        'unknown' as A_W
    from
        simplemvas.dcb_subscriptors as sub
                left join
        simplemvas.dcb_payments as pay ON sub.request_id2 = pay.request_id2 and pay.status = 2
                inner join
        simplemvas.dcb_transactions t ON sub.request_id2 = t.request_id2
    where
        sub.status != - 1
        #and dsub.year_week_start != (CURDATE() - INTERVAL WEEKDAY(CURDATE()) DAY)
        #and dpay.year_week_start != (CURDATE() - INTERVAL WEEKDAY(CURDATE()) DAY)
        and sub.product in ('mihoroscopo')
        and t.source in ('wigetmedia','mobusi','inmobi','exoclick')
        #and sub.subStartDate >= '2015-01-19'
        #and dpay.year_week_start >= '2015-01-26'
    group by lower(sub.product),lower(t.source),cohort,payWeek
    order by lower(sub.product),lower(t.source),cohort,payWeek
) p
inner join simplemvas.cohort_subs_day cs on cs.cohort=p.cohort and lower(cs.product)=lower(p.product) and lower(cs.source)=lower(p.source)
where p.payWeek is not null
group by lower(p.product), lower(p.source), p.cohort, p.payWeek
order by lower(p.product), lower(p.source), p.cohort, p.payWeek # ORDER is important to php script! 