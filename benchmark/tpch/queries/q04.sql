SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND o_orderbucket >= date_diff('day', DATE '1970-01-01', DATE '1993-07-01') // 10
    AND o_orderbucket <= date_diff('day', DATE '1970-01-01', DATE '1993-10-01') // 10
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
