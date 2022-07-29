from mrjob.job import MRJob
import time 

class partA2(MRJob):
	def mapper(self, _,line):
		try:
			fields = line.split(',')
			if len(fields) ==7:
				time_epoch = int(fields[6])
				value = int(fields[3])
				month = time.strftime("%m", time.gmtime(time_epoch))
				year = time.strftime("%y", time.gmtime(time_epoch))
				yield((month,year),(value,1))
		except:
			pass

	def combiner(self,key,value):
		count = 0
		total = 0
		for i in value:
			count+= i[0]
			total+= i[1]
		
		yield (key,(count,total))

	def reducer(self, key, value):
		count = 0
		total = 0
		for i in value:
			count += i[0]
			total += i[1]

		yield (key, (total/count))

if __name__ == '__main__':
	partA2.run()
