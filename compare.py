import pyspark
import re

def transact(line):
        try:
                fields = line.split(',')
                if len(fields)!=7:
                        return False
                int(fields[3])
                return True

        except:
                return False

def contract(line):
        try:
                fields = line.split(',')
                if len(fields)!=5:
                        return False
                return True
        except:
                return False

sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions")
valid_t = transactions.filter(transact)
map_t= valid_t.map(lambda i : (i.split(',')[2], int(i.split(',')[3])))
aggregate_t = map_t.reduceByKey(lambda c,d : c+d)
contracts = sc.textFile("/data/ethereum/contracts")
valid_c = contracts.filter(contract)
map_c = valid_c.map(lambda j: (j.split(',')[0], None))
joined = aggregate_t.join(map_c)

t10 = joined.takeOrdered(10, key = lambda l: -l[1][0])


with open('compare.txt', 'w') as f:
        for value in t10:
                f.write("{}:{}\n".format(value[0],value[1][0]))

