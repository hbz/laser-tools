#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# initial import for ISIL into EDB using the lobid api
#
# CAUTION: 
# deletes existing ISIL values in EDB
#
# 2017-04-12
# david.klober@hbz-nrw.de

from Config import Config
import MySQLdb as mdb
import requests
import string
import sys

class EdbIsilWriter:
	
	# set False to write to DB
	dryRun         = True 

	resultFile     = ''
	resultFileName = 'result.ISIL.txt'

	listValid    = []
	listMultiple = []
	listEmpty    = []

	lobidURI = 'http://lobid.org/organisation'

	def do(self):

		self.resultFile = open(self.resultFileName, 'w')

		self.process()
		self.printStats()
		self.writeFile()


	def process(self):

		try:
			config = Config()
			con = mdb.connect(config.dbhost, config.dbuser, config.dbpass, config.dbname, charset='utf8')
			cur = con.cursor(mdb.cursors.DictCursor)

			if(False == self.dryRun):
				cur.execute("UPDATE Teilnehmer SET ISIL = null")

			cur.execute("SELECT * FROM Teilnehmer")

			rows = cur.fetchall()
			for row in rows:
				print '%s > %s - %s, %s %s' % (row['Sigel'], row['Name'],  row['Strasse'],  row['PLZ'], row['Ort'])

				# query by sigel
				# removing whitespace 
				sigel = string.replace(row['Sigel'], ' ', '')

				# and add leading 'DE-' to x
				rCheck = False
				for r in rows:
					if ('DE-' + row['Sigel']) == r['Sigel']:
						rCheck = True

				# if no 'DE-x' exists
				if not rCheck:
					if sigel.find('DE-') != 0:
						sigel = 'DE-' + sigel

				qstr = self.lobidURI + '?id=' + string.replace(sigel, ' ', '') + '&format=ids'.encode('UTF-8')
				self.resultFile.write('.. %s \n' % qstr.encode('UTF-8'))

				res = requests.get(qstr)
				json = res.json()

				if len(json) == 0:
					query = row['Name'].split('<br>')
					if len(query) > 0:
						query = query[0]

					# fallback: query by name
					qstr = self.lobidURI + '?name=' + query + '&format=ids'.encode('UTF-8')
					self.resultFile.write('.... %s \n' % qstr.encode('UTF-8'))

					res = requests.get(qstr)
					json = res.json()

					if len(json) == 0:
						self.listEmpty.append([row['Sigel'], row['Name']])

				if len(json) == 1:
					for js in json:
						isil = string.replace(js['value'], self.lobidURI + '/', '')
						self.listValid.append([row['Sigel'], row['Name'], isil, js['label']])

						if(False == self.dryRun):
							cur.execute("UPDATE Teilnehmer SET ISIL = %s WHERE Sigel = %s", (isil, row['Sigel']))


				if len(json) > 1:
					listTmp = []
					for js in json:
						listTmp.append([row['Sigel'], row['Name'], string.replace(js['value'], self.lobidURI + '/', ''), js['label']])
					self.listMultiple.append(listTmp)


		except mdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)

		finally:
			if con:
				con.close()


	def printStats(self):

		print '\n'
		print 'DryRun: %s' % self.dryRun
		print 'Datei:  %s' % self.resultFileName
		print '\n'
		print '%d gültige Treffer'   % (len(self.listValid))
		print '%d multiple Treffer'  % (len(self.listMultiple))
		print '%d mal keine Treffer' % (len(self.listEmpty))
		print '\n'


	def writeFile(self):

		self.resultFile.write('\n %s gültige Treffer'   % len(self.listValid))
		self.resultFile.write('\n %s multiple Treffer'  % len(self.listMultiple))
		self.resultFile.write('\n %s mal keine Treffer' % len(self.listEmpty))

		self.resultFile.write('\n\n')

		self.resultFile.write('\n ----- Gültige Treffer ----- \n\n')

		for item in self.listValid:
			self.resultFile.write("%s - %s >>> %s - %s \n" % (item[0].encode('UTF-8'), item[1].encode('UTF-8'), item[2].encode('UTF-8'), item[3].encode('UTF-8')))

		self.resultFile.write('\n ----- Multiple Treffer ----- \n\n')

		for innerlist in self.listMultiple:
			for item in innerlist:
				self.resultFile.write("%s - %s >>> %s - %s \n" % (item[0].encode('UTF-8'), item[1].encode('UTF-8'), item[2].encode('UTF-8'), item[3].encode('UTF-8')))

		self.resultFile.write('\n ----- Keine gültigen Treffer ----- \n\n')

		for item in self.listEmpty:
			self.resultFile.write("%s - %s \n" % (item[0].encode('UTF-8'), item[1].encode('UTF-8')))


eiw = EdbIsilWriter()
eiw.do()
