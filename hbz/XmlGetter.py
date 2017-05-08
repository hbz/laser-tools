#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# generating xml from EDB data
#
# 2017-02-xx
# david.klober@hbz-nrw.de

from Config import Config
import MySQLdb as mdb
import dicttoxml
import re
import string
import sys

class Institution:
	user_name = ''
	title = ''
	street = ''
	zip = ''
	city = ''
	pobox = '' # edb
	country = ''
	county = ''
	contactperson = ''
	email = ''
	telephone = ''
	fax = ''
	url = ''
	#ipv4_allow 
	#ipv4_deny
	#shib_provider_id
	sigel = ''
	#ezb_id
	subscriper_group = ''
	uid = ''
	#mtime
	#status

class XmlGetter:
	
	fileValid 		 = ''
	fileValidName 	 = 'result.Institutions.xml'
	fileNonValid 	 = ''
	fileNonValidName = 'result.Institutions.nonValid.xml'

	institutions = []
	nonValidInstitutions = []


	def do(self):

		self.fileValid    = open(self.fileValidName, 'w')
		self.fileNonValid = open(self.fileNonValidName, 'w')

		self.process()
		self.printStats()

		funcRenameItems = lambda x: "institution" if 'institution' in x else 'token'
		
		xml1 = dicttoxml.dicttoxml(self.institutions, custom_root='institutions', item_func=funcRenameItems)
		self.fileValid.write(xml1)

		xml2 = dicttoxml.dicttoxml(self.nonValidInstitutions, custom_root='nonValidInstitutions', item_func=funcRenameItems)
		self.fileNonValid.write(xml2)


	def process(self):

		try:
			config = Config()
			con = mdb.connect(config.dbhost, config.dbuser, config.dbpass, config.dbname, charset='utf8')
			cur = con.cursor(mdb.cursors.DictCursor)

			cur.execute("SELECT t.*, l.Bundesland, l.Staat FROM Teilnehmer t join Land l on t.Land_ID = l.id")

			for instRow in cur.fetchall():

				inst = Institution()

				inst.title            = self.norm(instRow['Name'])
				inst.street           = self.norm(instRow['Strasse'])
				inst.zip              = self.norm(instRow['PLZ'])
				inst.city             = self.norm(instRow['Ort'])
				inst.country          = self.norm(instRow['Staat'])
				inst.subscriper_group = self.norm(instRow['Art'])

				if instRow['Postfach']:
					inst.pobox = self.norm(instRow['Postfach'])

				if instRow['Bundesland']:
					inst.county = self.norm(instRow['Bundesland'])
				
				if instRow['URL']:
					inst.url = self.norm(instRow['URL'])
				if instRow['ISIL']:
					inst.sigel = self.norm(instRow['ISIL'])
				if instRow['UID']:
					inst.uid = self.norm(instRow['UID'])
				if instRow['WIB']:
					inst.user_name = self.norm(instRow['WIB'])

				if instRow['Sigel']:
					cur.execute("SELECT * FROM Ansprechpartner WHERE Teilnehmer_Sigel = '" + instRow['Sigel'] + "'")

					ansprechpartner = cur.fetchall()

					if len(ansprechpartner) == 1:
						for ap in ansprechpartner:
							inst.contactperson = self.norm("%s %s" % (ap['Vorname'], ap['Name']))
							inst.email = self.norm(ap['email'])
							inst.telephone = self.norm(ap['Telefon'])
							inst.fax  = self.norm(ap['FAX'])
					
					elif len(ansprechpartner) > 1:
						inst.contactperson = []
						inst.email = []
						inst.telephone = []
						inst.fax = []

						for ap in ansprechpartner:
							inst.contactperson.append( self.norm("%s %s" % (ap['Vorname'], ap['Name'])))
							inst.email.append( self.norm(ap['email']))
							inst.telephone.append( self.norm(ap['Telefon']))
							inst.fax.append( self.norm(ap['FAX']))
					

				if self.check(inst):
					self.institutions.append(inst.__dict__)
				else:
					self.nonValidInstitutions.append(inst.__dict__)

		except mdb.Error, e:
		  
			print "Error %d: %s" % (e.args[0], e.args[1])
			sys.exit(1)
			
		finally:    
				
			if con:    
				con.close()


	def printStats(self):

		all = len(self.institutions) + len(self.nonValidInstitutions)

		print '\n\n'
		print 'Datei:  %s' % self.fileValidName
		print 'Gültig: %d von %d \n' % (len(self.institutions), all)

		#for inst in self.institutions:
		#	print inst.__dict__

		print 'Datei:  %s' % self.fileNonValidName
		print 'Ungültig: %d von %d \n' %(len(self.nonValidInstitutions), all)

		#for inst in self.nonValidInstitutions:
		#	print inst.__dict__


	def norm(self, str):

		str = re.sub('\s*<br\s?/?>\s*', ' ', str)
		return str.encode('UTF-8')


	def check(self, institution):

		valid = True

		if not institution.sigel:
			valid = False

		return valid



xg = XmlGetter()
xg.do()
