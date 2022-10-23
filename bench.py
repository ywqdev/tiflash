import argparse
import pymysql
import threading
import time
import random
import datetime

target_addr = "127.0.0.1"
target_user = "root"
target_database = "tpch_100"
target_port = 6003
q2 = """select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and ps_supplycost = (
        select
            min(ps_supplycost)
        from
            partsupp, supplier,
            nation, region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
    )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey;"""
q6 = """select
    sum(l_extendedprice*l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24;"""
q11 = """select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
        select
            sum(ps_supplycost * ps_availqty) * 0.0001
        from
            partsupp,
            supplier,
            nation
        where
            ps_suppkey = s_suppkey
            and s_nationkey = n_nationkey
            and n_name = 'GERMANY'
        )
order by
value desc;"""
q12 = """select
    l_shipmode,
    sum(case
    when o_orderpriority ='1-URGENT'
    or o_orderpriority ='2-HIGH'
    then 1
    else 0
    end) as high_line_count,
    sum(case
    when o_orderpriority <> '1-URGENT'
    and o_orderpriority <> '2-HIGH'
    then 1
    else 0
    end) as low_line_count
from
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1994-01-01'
    and l_receiptdate < date '1994-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode;"""
q14 = """select
    100.00 * sum(case
    when p_type like 'PROMO%'
    then l_extendedprice*(1-l_discount)
    else 0
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
    l_partkey = p_partkey
    and l_shipdate >= date '1995-09-01'
    and l_shipdate < date '1995-09-01' + interval '1' month;"""
q19 = """select
    sum(l_extendedprice * (1 - l_discount) ) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 1 and l_quantity <= 1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 10 and l_quantity <= 10 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#34'
        and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 20 and l_quantity <= 20 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );"""
q22 = """select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from (
    select
        substring(c_phone from 1 for 2) as cntrycode,
        c_acctbal
    from
        customer
    where
        substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17')
        and c_acctbal > (
            select
                avg(c_acctbal)
                from
                customer
            where
                c_acctbal > 0.00
                and substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17')
        )
        and not exists (
            select
                *
            from
                orders
            where
                o_custkey = c_custkey
        )
) as custsale
group by
    cntrycode
order by
    cntrycode;"""


class Sql:
    def __init__(self, sql, sql_name):
        self.sql = sql
        self.sql_name = sql_name
        self.total_time = 0
        self.total_count = 0
        self.lock = threading.Lock()

    def getSqlName(self):
        return self.sql_name

    def getSql(self):
        return self.sql

    def addInfo(self, executed_time):
        self.lock.acquire()
        self.total_time += executed_time
        self.total_count += 1
        self.lock.release()

    def getAvgTime(self):
        self.lock.acquire()
        avg = 0
        if self.total_count != 0:
            avg = self.total_time / self.total_count
        self.lock.release()
        return avg

    def getCount(self):
        self.lock.acquire()
        count = self.total_count
        self.lock.release()
        return count

    def getInfo(self):
        info = "%s[avg time: %fs, total count:%d]" % (
            self.sql_name, self.getAvgTime(), self.getCount())
        return info


# sqls = {1: Sql(q2, "Q2"), 2: Sql(q6, "Q6"), 3: Sql(q12, "Q12"), 4: Sql(
#     q11, "Q11"), 5: Sql(q19, "Q19"), 6: Sql(q14, "Q14"), 7: Sql(q22, "Q22")}

sqls = {1: Sql(q6, "Q6"), 2: Sql(q12, "Q12"), 3: Sql(
    q11, "Q11"), 4: Sql(q19, "Q19"), 5: Sql(q14, "Q14"), 6: Sql(q22, "Q22")}


def runClient():
    print("%s start..." % threading.current_thread().getName())
    connection = pymysql.connect(host=target_addr, port=target_port, user=target_user,
                                 database=target_database, cursorclass=pymysql.cursors.DictCursor)
    isFirst = False
    with connection:
        with connection.cursor() as cursor:
            if isFirst == False:
                cursor.execute("set tidb_enforce_mpp=1;")
                isFirst = True
            for j in range(1, len(sqls) + 1):
                sql = sqls[j].getSql()
                print("Sql: ", sqls[j].getSqlName())
                start = time.time()
                cursor.execute(sql)
                end = time.time()
                sqls[j].addInfo(end - start)
                print("execution time: ", end - start)


def parse_args():
    description = "you should add those parameter"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--c_num', type=int, default=1,
                        help='client number, default 1')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    client_num = args.c_num

    threads = []
    while (client_num > 0):
        thread = threading.Thread(target=runClient)
        thread.start()
        threads.append(thread)
        client_num -= 1

    start = time.time()

    for thread in threads:
        thread.join()

    end = time.time()

    count = 0
    for sql in sqls.values():
        count += sql.getCount()
        print(sql.getInfo())

    total_time = end - start

    print("Total time: %f" % total_time)
    print("Total queries: %d, QPS: %f" % (count, (count / (end - start))))
    print("exit...")
