#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# initial import for UID and WIB into EDB
#
# CAUTION: 
# deletes existing UID and WIB values in EDB
# inserts UID and WIB if match via ISIL, Name or URL is found
#
# 2017-04-12
# david.klober@hbz-nrw.de

from Config import Config
import MySQLdb as mdb
import re
import xmltodict

class EdbUidAndWibWriter:
	
	# set False to write to DB
	dryRun         = True

	inputFile      = './_nlinstitutions.xml'
	resultFileName = 'result.UIDandWIB.txt'

	xml = [] # xml content as list of dictionaries
	edb = [] # edb content as list of dictionaries

	listSigelMatch = [] # edb teilnehmer matched by isil = nl:sigel
	listTitleMatch = [] # edb teilnehmer matched by name = nl:title
	listUrlMatch   = [] # edb teilnehmer matched by url

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
			institution['wib']   = inst['nl:user_name']
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
				cur.execute("UPDATE Teilnehmer SET WIB = null")

			cur.execute("SELECT * FROM Teilnehmer")

			rows = cur.fetchall()
			for row in rows:
				teilnehmer = {}
				teilnehmer['uid']   = row['UID']
				teilnehmer['wib']   = row['WIB']
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
				wib = inst['wib']

				if(tn['isil'] and inst['sigel']):
					deSigel = self.norm(inst['sigel'])

					if(deSigel.find('DE-') != 0):
						deSigel = 'DE-' + self.norm(inst['sigel'])

					if(self.norm(tn['isil']) == deSigel):
						print 'ISIL Match: %s' % tn['isil']
						self.matchedXmlList.append(uid)
						tn['uid'] = uid
						tn['wib'] = wib
						tn['match'].append('sigel')

				if(tn['name'] and inst['title']):
					if(self.norm(tn['name']) == self.norm(inst['title'])):
						print 'NAME Match: %s' % tn['name']
						self.matchedXmlList.append(uid)
						tn['uid'] = uid
						tn['wib'] = wib
						tn['match'].append('title')

				if(tn['url'] and inst['url']):
					if(self.norm(tn['url']) == self.norm(inst['url'])):
						print 'URL Match: %s' % tn['url']
						self.matchedXmlList.append(uid)
						tn['uid'] = uid
						tn['wib'] = wib
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

			if('sigel' in tn['match']):
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
		print 'Teilnehmer mit ISIL=Sigel Matches: %s' % len(self.listSigelMatch)
		print 'Teilnehmer mit Name=Title Matches: %s' % len(self.listTitleMatch)
		print 'Teilnehmer mit URL Matches:        %s' % len(self.listUrlMatch)
		print '\n'
		print 'Ignored from XML:   %s' % len(self.nonMatchingXmlList)
		print '\n'


	def writeToEDB(self):

		try:
			config = Config()
			con = mdb.connect(config.dbhost, config.dbuser, config.dbpass, config.dbname, charset='utf8')
			cur = con.cursor(mdb.cursors.DictCursor)

			for item in self.listSigelMatch:
				cur.execute("UPDATE Teilnehmer SET UID = %s, WIB = %s WHERE ISIL = %s", (item['uid'], item['wib'], item['isil']))

			for item in self.listTitleMatch:
				cur.execute("UPDATE Teilnehmer SET UID = %s, WIB = %s WHERE Name = %s", (item['uid'], item['wib'], item['name']))

			for item in self.listUrlMatch:
				cur.execute("UPDATE Teilnehmer SET UID = %s, WIB = %s WHERE URL = %s", (item['uid'], item['wib'], item['url']))

		except mdb.Error, e:
			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)

		finally:
			if con:
				con.close()


	def writeTeilnehmerItem(self, item):

		uid  = item['uid'] or ''
		name = item['name'] or ''
		isil = item['isil'] or ''
		wib  = item['wib'] or ''
		url  = item['url'] or ''
		self.resultFile.write("%s : %s : %s / %s - %s \n" % (uid.encode('UTF-8'), name.encode('UTF-8'), isil.encode('UTF-8'), wib.encode('UTF-8'), url.encode('UTF-8')))


	def writeFile(self):

		self.resultFile.write('\n %s Institutions in XML' % len(self.xml))
		self.resultFile.write('\n %s Teilnehmer in EDB'   % len(self.edb))

		self.resultFile.write('\n %s Teilnehmer mit ISIL=Sigel Matches'  % len(self.listSigelMatch))
		self.resultFile.write('\n %s Teilnehmer mit Name=Title Matches'  % len(self.listTitleMatch))
		self.resultFile.write('\n %s Teilnehmer mit URL Matches'         % len(self.listUrlMatch))

		self.resultFile.write('\n %s Ignored from XML'    % len(self.nonMatchingXmlList))

		self.resultFile.write('\n\n')

		self.resultFile.write('\n ----- Teilnehmer mit ISIL=Sigel Matches ----- \n\n')

		for item in self.listSigelMatch:
			self.writeTeilnehmerItem(item)

		self.resultFile.write('\n ----- Teilnehmer mit Name=Title Matches ----- \n\n')

		for item in self.listTitleMatch:
			self.writeTeilnehmerItem(item)

		self.resultFile.write('\n ----- Teilnehmer mit URL Matches ----- \n\n')

		for item in self.listUrlMatch:
			self.writeTeilnehmerItem(item)

		self.resultFile.write('\n ----- Ignored from XML ----- \n\n')

		for item in self.nonMatchingXmlList:
			uid  = item['uid'] or ''
			name = item['title'] or ''
			isil = item['sigel'] or ''
			wib  = item['wib'] or ''
			url  = item['url'] or ''
			self.resultFile.write("%s : %s : %s / %s - %s \n" % (uid.encode('UTF-8'), name.encode('UTF-8'), isil.encode('UTF-8'), wib.encode('UTF-8'), url.encode('UTF-8')))


euaww = EdbUidAndWibWriter()
euaww.do()
