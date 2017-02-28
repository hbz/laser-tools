#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# 2017-02-xx
# david.klober@hbz-nrw.de

from Config import Config
import MySQLdb as mdb
import re
import xmltodict

class EDBUIDWriter:
	
	# set False to write to DB
	dryRun         = True

	inputFile      = './_nlinstitutions.xml'
	resultFileName = 'result.UID.txt'
	xml = []
	edb = []

	listUidMatch   = []
	listSigelMatch = []
	listTitleMatch = []
	listUrlMatch   = []

	matchedXmlList     = []
	nonMatchingXmlList = []


	def do(self):

		self.resultFile = open(self.resultFileName, 'w')

		self.readXML()
		self.readEDB()

		self.process()
		self.stats()

		if(False == self.dryRun):
			self.writeToEDB()
		
		self.writeFile()


	def readXML(self):

		with open(self.inputFile) as ff:
			result = xmltodict.parse(ff.read())

		for inst in result['nl:institutions']['nl:institution']:
			institution = {}
			institution['uid']   = inst['nl:uid']
			institution['sigel'] = inst['nl:sigel']
			institution['title'] = inst['nl:title']
			institution['url']   = inst['nl:url']

			self.xml.append(institution)


	def readEDB(self):

		try:
			config = Config()
			con = mdb.connect(config.dbhost, config.dbuser, config.dbpass, config.dbname, charset='utf8')
			cur = con.cursor(mdb.cursors.DictCursor)

			if(False == self.dryRun):
				cur.execute("UPDATE Teilnehmer SET UID = null")

			cur.execute("SELECT * FROM Teilnehmer")

			rows = cur.fetchall()
			for row in rows:
				teilnehmer = {}
				teilnehmer['uid']   = row['UID']
				teilnehmer['isil']  = row['ISIL']
				teilnehmer['name']  = row['Name']
				teilnehmer['url']   = row['URL']
				teilnehmer['match'] = []

				self.edb.append(teilnehmer)

		except mdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)

		finally:
			if con:
				con.close()


	def process(self):

		# find matches
		for tn in self.edb:
			for inst in self.xml:

				uid = inst['uid']

				if(tn['uid'] and inst['uid']):
					if(self.norm(tn['uid']) == self.norm(inst['uid'])):
						print 'UID Match: %s' % tn['uid']
						self.matchedXmlList.append(uid)
						tn['match'].append('uid')

				if(tn['isil'] and inst['sigel']):
					deSigel = self.norm(inst['sigel'])

					if(deSigel.find('DE-') != 0):
						deSigel = 'DE-' + self.norm(inst['sigel'])

					if(self.norm(tn['isil']) == deSigel):
						print 'ISIL Match: %s' % tn['isil']
						self.matchedXmlList.append(uid)
						tn['uid'] = uid
						tn['match'].append('sigel')

				if(tn['name'] and inst['title']):
					if(self.norm(tn['name']) == self.norm(inst['title'])):
						print 'NAME Match: %s' % tn['name']
						self.matchedXmlList.append(uid)
						tn['uid'] = uid
						tn['match'].append('title')

				if(tn['url'] and inst['url']):
					if(self.norm(tn['url']) == self.norm(inst['url'])):
						print 'URL Match: %s' % tn['url']
						self.matchedXmlList.append(uid)
						tn['uid'] = uid
						tn['match'].append('url')

		
		for inst in self.xml:
			found = False
			for mxl in self.matchedXmlList:
				if(inst['uid'] and inst['uid'] == mxl):
					found = True
			
			# remember non matches
			if not found:
				self.nonMatchingXmlList.append(inst)


		# sorting
		for tn in self.edb:

			if('uid' in tn['match']):
				self.listUidMatch.append(tn)

			elif('sigel' in tn['match']):
				self.listSigelMatch.append(tn)

			elif('title' in tn['match']):
				self.listTitleMatch.append(tn)

			elif('url' in tn['match']):
				self.listUrlMatch.append(tn)


	def norm(self, str):

		str = re.sub('[\s+]', '', str)
		str = re.sub('\s*<br\s?/?>\s*', ' ', str)

		return str


	def stats(self):

		print '\n'
		print 'DryRun: %s' % self.dryRun
		print 'Datei:  %s' % self.resultFileName
		print '\n'
		print 'Institutions in XML: %d' % len(self.xml)
		print 'Teilnehmer in EDB:   %d' % len(self.edb)
		print '\n'
		print 'UID Matches:        %s' % len(self.listUidMatch)
		print 'Sigel=ISIL Matches: %s' % len(self.listSigelMatch)
		print 'Title=Name Matches: %s' % len(self.listTitleMatch)
		print 'URL Matches:        %s' % len(self.listUrlMatch)
		print '\n'
		print 'Ignored from XML:   %s' % len(self.nonMatchingXmlList)
		print '\n'


	def writeToEDB(self):

		try:
			config = Config()
			con = mdb.connect(config.dbhost, config.dbuser, config.dbpass, config.dbname, charset='utf8')
			cur = con.cursor(mdb.cursors.DictCursor)

			for item in self.listSigelMatch:
				cur.execute("UPDATE Teilnehmer SET UID = %s WHERE ISIL = %s", (item['uid'], item['isil']))

			for item in self.listTitleMatch:
				cur.execute("UPDATE Teilnehmer SET UID = %s WHERE Name = %s", (item['uid'], item['name']))

			for item in self.listUrlMatch:
				cur.execute("UPDATE Teilnehmer SET UID = %s WHERE URL = %s", (item['uid'], item['url']))

		except mdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)

		finally:
			if con:
				con.close()


	def writeItem(self, item):

		uid  = item['uid'] or ''
		name = item['name'] or ''
		isil = item['isil'] or ''
		url  = item['url'] or ''
		self.resultFile.write("%s : %s / %s / %s \n" % (uid.encode('UTF-8'), name.encode('UTF-8'), isil.encode('UTF-8'), url.encode('UTF-8')))


	def writeFile(self):

		self.resultFile.write('\n %s Institutions in XML' % len(self.xml))
		self.resultFile.write('\n %s Teilnehmer in EDB'   % len(self.edb))

		self.resultFile.write('\n %s UID Matches'         % len(self.listUidMatch))
		self.resultFile.write('\n %s Sigel=ISIL Matches'  % len(self.listSigelMatch))
		self.resultFile.write('\n %s Title=Name Matches'  % len(self.listTitleMatch))
		self.resultFile.write('\n %s URL Matches'         % len(self.listUrlMatch))

		self.resultFile.write('\n %s Ignored from XML'    % len(self.nonMatchingXmlList))

		self.resultFile.write('\n\n')

		self.resultFile.write('\n ----- UID Matches ----- \n\n')

		for item in self.listUidMatch:
			self.writeItem(item)

		self.resultFile.write('\n ----- Sigel=ISIL Matches ----- \n\n')

		for item in self.listSigelMatch:
			self.writeItem(item)

		self.resultFile.write('\n ----- Title=Name Matches ----- \n\n')

		for item in self.listTitleMatch:
			self.writeItem(item)

		self.resultFile.write('\n ----- URL Matches ----- \n\n')

		for item in self.listUrlMatch:
			self.writeItem(item)

		self.resultFile.write('\n ----- Ignored from XML ----- \n\n')

		for item in self.nonMatchingXmlList:
			uid  = item['uid'] or ''
			name = item['title'] or ''
			isil = item['sigel'] or ''
			url  = item['url'] or ''
			self.resultFile.write("%s : %s / %s / %s \n" % (uid.encode('UTF-8'), name.encode('UTF-8'), isil.encode('UTF-8'), url.encode('UTF-8')))


euw = EDBUIDWriter()
euw.do()
