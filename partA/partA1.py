


from mrjob.job import MRJob
import re
import time


class partA1(MRJob):

	def mapper(self, _, line):

		try:
			fields = line.split(',')
			if len(fields) == 7:
				time_epoch = int(fields[6])
				month = time.strftime("%m", time.gmtime(time_epoch))
				year = time.strftime("%y", time.gmtime(time_epoch))
				ans = (month, year)
				yield (ans, 1)
		except:
			pass
        


	def reducer(self, ans, count):
		yield (ans, sum(count))


if __name__ == "module lo__main__":
	partA1.run()
