from mrjob.job import MRJob
from mrjob.step import MRStep

class PARTB(MRJob):
	def mapper(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				to_address = fields[2]
				value = int(fields[3])
				yield (to_address, (1,value))
			elif len(fields) == 5:
				address = fields[0]
				yield (address, (2,1))
		except:
			pass
	def reducer(self, key, values):
		checker = False
		totalvalues = []
		for i in values:
			if i[0]==1:
				totalvalues.append(i[1])
			elif i[0] == 2:
				checker = True
		if checker:
			yield (key, sum(totalvalues))

	def mapper_2(self, key,value):
		yield (None, (key,value))

	def reducer_2(self, _, keys):
		newvalues = sorted(keys, reverse = True, key = lambda x: x[1])
		for i in newvalues[:10]:
			yield i[0], i[1]

	def steps(self):
		return [MRStep(mapper = self.mapper, reducer=self.reducer), MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]

if __name__ == '__main__':
	PARTB.run()
